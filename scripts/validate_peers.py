"""
EXPERIMENT (standalone, imports nothing from the rest of the project)

Takes the peer list found by discover_peers.py and checks which peers we can REALLY download from.
Each peer goes through these stages, and we record the stage where it failed:

  1. connect     : TCP connection opens
  2. handshake   : peer answers with the same info_hash (= it is in OUR torrent's swarm)
  3. pieces      : peer tells us which pieces it has (bitfield / have) and has at least one
  4. unchoke     : we send 'interested', peer answers 'unchoke' (= it is willing to upload to us)
  5. block       : we request one 16 KiB block and the peer really sends it  -> DOWNLOADABLE

Usage:  python scripts/validate_peers.py [torrent_file] [peers_file]   (from v2_asyncio/)
Defaults: peers from data/all_peers.txt, result saved to data/validated_peers.txt
"""
import sys
import os
import time
import socket
import struct
import hashlib
import bencodepy
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

CONNECT_TIMEOUT = 5     # seconds to open the TCP connection
STEP_TIMEOUT = 6        # seconds we wait for each next message from the peer
UNCHOKE_WAIT = 10       # seconds we wait for unchoke after sending interested
WORKERS = 100           # peers validated at the same time
BLOCK_SIZE = 16384

MY_PEER_ID = b"-VP0001-" + os.urandom(12)


def load_torrent(path):
    with open(path, "rb") as f:
        torrent = bencodepy.decode(f.read())
    info = torrent[b"info"]
    info_hash = hashlib.sha1(bencodepy.encode(info)).digest()
    total_pieces = len(info[b"pieces"]) // 20
    return info_hash, total_pieces


def load_peers(path):
    peers = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                ip, port = line.rsplit(":", 1)
                peers.append((ip, int(port)))
    return peers


def recv_exact(sock, n):
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("closed")
        data.extend(chunk)
    return bytes(data)


def read_message(sock):
    """Returns (msg_id, payload). Keep-alive is (None, b'')."""
    length = struct.unpack(">I", recv_exact(sock, 4))[0]
    if length == 0:
        return None, b""
    if length > 1024 * 1024 + 16:  # bigger than any sane bitfield/piece message
        raise ConnectionError("message too large")
    message = recv_exact(sock, length)
    return message[0], message[1:]


def parse_bitfield(data, total_pieces):
    pieces = set()
    for byte_idx, byte in enumerate(data):
        for bit_idx in range(8):
            if byte & (1 << (7 - bit_idx)):
                index = byte_idx * 8 + bit_idx
                if index < total_pieces:
                    pieces.add(index)
    return pieces


def validate_peer(peer, info_hash, total_pieces):
    """Returns dict: {peer, stage, ok, pieces, speed_ms} where `stage` is how far the peer got."""
    ip, port = peer
    result = {"peer": peer, "stage": "connect", "ok": False, "pieces": 0, "ms": 0}
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    start = time.time()

    try:
        # ---- 1. connect ----
        sock.settimeout(CONNECT_TIMEOUT)
        sock.connect((ip, port))
        result["stage"] = "handshake"

        # ---- 2. handshake ----
        sock.sendall(bytes([19]) + b"BitTorrent protocol" + b"\x00" * 8 + info_hash + MY_PEER_ID)
        sock.settimeout(STEP_TIMEOUT)
        response = recv_exact(sock, 68)
        if response[0] != 19 or response[1:20] != b"BitTorrent protocol" or response[28:48] != info_hash:
            return result  # not BitTorrent, or a different torrent
        fast_extension = bool(response[27] & 0x04)  # peer may send have_all / have_none instead of bitfield
        result["stage"] = "pieces"

        # ---- 3. pieces: read messages until we know what the peer has ----
        pieces = set()
        got_piece_info = False
        unchoked = False
        deadline = time.time() + STEP_TIMEOUT
        while time.time() < deadline and not got_piece_info:
            msg_id, payload = read_message(sock)
            if msg_id == 5:      # bitfield
                pieces = parse_bitfield(payload, total_pieces)
                got_piece_info = True
            elif msg_id == 14:   # have_all (fast extension): seeder
                pieces = set(range(total_pieces))
                got_piece_info = True
            elif msg_id == 15:   # have_none (fast extension)
                got_piece_info = True
            elif msg_id == 4 and len(payload) == 4:  # have
                pieces.add(struct.unpack(">I", payload)[0])
            elif msg_id == 1:    # unchoke already
                unchoked = True
            elif msg_id is not None and pieces:
                got_piece_info = True
            # peers with 0 pieces may send nothing: loop until the deadline

        pieces = {p for p in pieces if p < total_pieces}
        result["pieces"] = len(pieces)
        if not pieces:
            return result  # nothing to download from this peer

        # ---- 4. unchoke ----
        result["stage"] = "unchoke"
        sock.sendall(struct.pack(">IB", 1, 2))  # interested
        unchoke_deadline = time.time() + UNCHOKE_WAIT
        sock.settimeout(1)
        while not unchoked and time.time() < unchoke_deadline:
            try:
                msg_id, payload = read_message(sock)
            except socket.timeout:
                continue
            if msg_id == 1:
                unchoked = True
            elif msg_id == 4 and len(payload) == 4:
                pieces.add(struct.unpack(">I", payload)[0])
            elif msg_id == 5:
                pieces = parse_bitfield(payload, total_pieces)
            result["pieces"] = len(pieces)
        if not unchoked:
            return result  # peer is choking us

        # ---- 5. block: really ask for data ----
        result["stage"] = "block"
        piece_index = min(pieces)
        sock.sendall(struct.pack(">IBIII", 13, 6, piece_index, 0, BLOCK_SIZE))
        block_deadline = time.time() + STEP_TIMEOUT
        while time.time() < block_deadline:
            try:
                msg_id, payload = read_message(sock)
            except socket.timeout:
                continue
            if msg_id == 7 and len(payload) > 8:   # piece message with real data
                result["stage"] = "downloadable"
                result["ok"] = True
                result["ms"] = int((time.time() - start) * 1000)
                return result
            if msg_id == 0:                        # choked us again
                return result
            if msg_id == 16:                       # reject request (fast extension)
                return result
        return result

    except socket.timeout:
        result["stage"] += "_timeout"
    except (ConnectionError, OSError):
        result["stage"] += "_error"
    except Exception:
        result["stage"] += "_error"
    finally:
        sock.close()

    return result


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    data_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
    os.makedirs(data_dir, exist_ok=True)
    torrent_path = sys.argv[1] if len(sys.argv) > 1 else os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "torrents", "big-buck-bunny.torrent"))
    peers_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(data_dir, "all_peers.txt")

    info_hash, total_pieces = load_torrent(torrent_path)
    peers = load_peers(peers_path)
    print(f"[torrent] {torrent_path} ({total_pieces} pieces)")
    print(f"[peers]   {len(peers)} peers loaded from {peers_path}")
    print(f"[start]   validating with {WORKERS} parallel connections...\n")

    t0 = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(validate_peer, p, info_hash, total_pieces) for p in peers]
        for i, future in enumerate(futures, 1):
            results.append(future.result())
            if i % 50 == 0 or i == len(futures):
                good = sum(r["ok"] for r in results)
                print(f"  [progress] {i}/{len(futures)} checked | {good} downloadable")
    elapsed = time.time() - t0

    # ---- Report ----
    stages = Counter(r["stage"] for r in results)
    good = sorted((r for r in results if r["ok"]), key=lambda r: (-r["pieces"], r["ms"]))
    handshake_ok = sum(1 for r in results if not r["stage"].startswith(("connect", "handshake")))
    seeders = sum(1 for r in good if r["pieces"] == total_pieces)

    print("\n================ RESULT ================")
    print(f"Peers checked          : {len(results)}  in {elapsed:.1f}s")
    print(f"Handshake OK           : {handshake_ok}  (real BitTorrent peers of THIS torrent)")
    print(f"Have pieces + unchoked : {sum(1 for r in results if r['stage'] in ('block', 'downloadable'))}")
    print(f"DOWNLOADABLE           : {len(good)}  (sent us a real data block)")
    print(f"  of which seeders     : {seeders}  (have 100% of pieces)")
    print(f"Usable rate            : {len(good) / len(results):.1%}")

    print("\nWhere the rest failed (stage -> count):")
    for stage, count in stages.most_common():
        if stage != "downloadable":
            print(f"  {stage:20s} {count}")

    out_file = os.path.join(data_dir, "validated_peers.txt")
    with open(out_file, "w") as f:
        for r in good:
            f.write(f"{r['peer'][0]}:{r['peer'][1]}\n")
    print(f"\nSaved {len(good)} downloadable peers -> {out_file}")

    print("\nBest peers (most pieces first):")
    for r in good[:10]:
        print(f"  {r['peer'][0]}:{r['peer'][1]:<6} pieces {r['pieces']}/{total_pieces}  first block in {r['ms']} ms")
