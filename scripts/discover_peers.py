"""
EXPERIMENT (standalone, imports nothing from the rest of the project)

Goal: find peers for a torrent WITHOUT a tracker, in two steps:
  1. DHT (BEP 5)  : ask the global Kademlia network "who has this info_hash?"
  2. PEX (BEP 11) : connect to a few of those peers and ask them for THEIR peers

Usage:  python scripts/discover_peers.py [torrent_file]   (from v2_asyncio/)
Output: data/dht_peers.txt, pex_peers.txt, all_peers.txt
"""
import sys
import time
import socket
import struct
import random
import hashlib
import os
import bencodepy
from concurrent.futures import ThreadPoolExecutor

BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.libtorrent.org", 25401),
    ("router.silotis.us", 6881),
]

DHT_SECONDS = 30      # how long the DHT lookup runs
DHT_ALPHA = 16        # queries sent per round (parallelism of the lookup)
PEX_PROBES = 40       # how many DHT peers we connect to for PEX
PEX_WAIT = 45         # seconds we wait for a peer to send its ut_pex message

MY_PEER_ID = b"-EX0001-" + os.urandom(12)
MY_NODE_ID = os.urandom(20)


def load_info_hash(path):
    """Only thing we need from the .torrent file is the info_hash."""
    with open(path, "rb") as f:
        torrent = bencodepy.decode(f.read())
    return hashlib.sha1(bencodepy.encode(torrent[b"info"])).digest(), torrent


def parse_compact_peers(data):
    """6 bytes per peer: 4 bytes ip + 2 bytes port."""
    peers = set()
    for i in range(0, len(data) - 5, 6):
        ip = socket.inet_ntoa(data[i : i + 4])
        port = struct.unpack(">H", data[i + 4 : i + 6])[0]
        if port != 0:
            peers.add((ip, port))
    return peers


# ============================================================
# Part 1: DHT
# ============================================================

def parse_compact_nodes(data):
    """26 bytes per node: 20 bytes node id + 4 bytes ip + 2 bytes port."""
    nodes = []
    for i in range(0, len(data) - 25, 26):
        node_id = data[i : i + 20]
        ip = socket.inet_ntoa(data[i + 20 : i + 24])
        port = struct.unpack(">H", data[i + 24 : i + 26])[0]
        if port != 0:
            nodes.append((node_id, (ip, port)))
    return nodes


def dht_get_peers(info_hash):
    """Iterative Kademlia lookup: always query the nodes CLOSEST (XOR distance) to info_hash.
    Each reply gives either peers ('values') or even closer nodes ('nodes')."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.5)

    target = int.from_bytes(info_hash, "big")
    candidates = {}   # addr -> distance to info_hash (smaller = closer)
    queried = set()
    peers = set()
    responded = 0

    bootstrap = []
    for host, port in BOOTSTRAP_NODES:
        try:
            bootstrap.append((socket.gethostbyname(host), port))
        except socket.gaierror:
            print(f"  [dht] cannot resolve {host}")
    for addr in bootstrap:
        candidates[addr] = 1 << 160  # id unknown: "far away"

    start = time.time()
    round_no = 0
    while time.time() - start < DHT_SECONDS:
        # pick the closest nodes we have not asked yet
        todo = sorted((a for a in candidates if a not in queried), key=lambda a: candidates[a])[:DHT_ALPHA]
        if not todo:
            # Ran out of nodes (bootstrap replies can be tiny/lost): ask the bootstrap nodes again,
            # they return a different random set of nodes each time.
            for addr in bootstrap:
                queried.discard(addr)
            time.sleep(1)
            continue
        round_no += 1

        for addr in todo:
            queried.add(addr)
            query = {
                b"t": os.urandom(2),
                b"y": b"q",
                b"q": b"get_peers",
                b"a": {b"id": MY_NODE_ID, b"info_hash": info_hash},
            }
            try:
                sock.sendto(bencodepy.encode(query), addr)
            except OSError:
                pass

        # collect replies for up to 2 seconds
        batch_end = time.time() + 2
        while time.time() < batch_end:
            try:
                data, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                continue

            try:
                reply = bencodepy.decode(data)
                if reply.get(b"y") != b"r":
                    continue
                r = reply[b"r"]
            except Exception:
                continue

            responded += 1
            for value in r.get(b"values", []):
                if isinstance(value, bytes):
                    peers |= parse_compact_peers(value)

            for node_id, node_addr in parse_compact_nodes(r.get(b"nodes", b"")):
                candidates[node_addr] = int.from_bytes(node_id, "big") ^ target

        print(f"  [dht] round {round_no:2d} | queried {len(queried):4d} | replies {responded:4d} | peers {len(peers):4d}")

    sock.close()
    return peers, len(queried), responded


# ============================================================
# Part 2: PEX (needs the BEP 10 "extension protocol" on top of the normal handshake)
# ============================================================

def recv_exact(sock, n):
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("closed")
        data.extend(chunk)
    return bytes(data)


def pex_probe(peer, info_hash):
    """Connect to one peer and wait for its ut_pex message.
    Returns (status, set_of_peers). status: 'pex' | 'no_ltep' | 'no_pex_msg' | 'failed'"""
    ip, port = peer
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(5)
        sock.connect((ip, port))

        # Normal handshake, but reserved byte 5 has bit 0x10 set = "I support the extension protocol"
        reserved = bytes([0, 0, 0, 0, 0, 0x10, 0, 0])
        sock.sendall(bytes([19]) + b"BitTorrent protocol" + reserved + info_hash + MY_PEER_ID)

        response = recv_exact(sock, 68)
        if response[28:48] != info_hash:
            return "failed", set()
        if not response[25] & 0x10:
            return "no_ltep", set()

        # Extended handshake: message id 20, extension id 0, payload = bencoded dict.
        # "m" tells the peer which id WE want it to use when it sends us ut_pex messages: 1
        payload = bencodepy.encode({b"m": {b"ut_pex": 1}, b"v": b"pex-experiment"})
        body = bytes([20, 0]) + payload
        sock.sendall(struct.pack(">I", len(body)) + body)

        deadline = time.time() + PEX_WAIT
        sock.settimeout(5)
        while time.time() < deadline:
            try:
                length = struct.unpack(">I", recv_exact(sock, 4))[0]
                if length == 0:
                    continue  # keep-alive
                if length > 1024 * 1024:
                    return "failed", set()
                message = recv_exact(sock, length)
            except socket.timeout:
                continue

            if message[0] != 20:  # not an extended message (bitfield, have, ...): ignore
                continue

            ext_id, ext_payload = message[1], message[2:]
            if ext_id == 1:  # our ut_pex id -> this is the PEX message
                pex = bencodepy.decode(ext_payload)
                found = parse_compact_peers(pex.get(b"added", b""))
                return "pex", found

        return "no_pex_msg", set()

    except Exception:
        return "failed", set()
    finally:
        sock.close()


def pex_discover(seed_peers, info_hash):
    probes = list(seed_peers)[:PEX_PROBES]
    print(f"  [pex] probing {len(probes)} peers in parallel (waiting up to {PEX_WAIT}s each)...")

    stats = {"pex": 0, "no_ltep": 0, "no_pex_msg": 0, "failed": 0}
    found = set()
    with ThreadPoolExecutor(max_workers=len(probes) or 1) as pool:
        for status, peers in pool.map(lambda p: pex_probe(p, info_hash), probes):
            stats[status] += 1
            found |= peers
    return found, stats


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    torrent_path = sys.argv[1] if len(sys.argv) > 1 else os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "torrents", "big-buck-bunny.torrent"))

    info_hash, torrent = load_info_hash(torrent_path)
    print(f"[torrent] {torrent_path}")
    print(f"[torrent] info_hash: {info_hash.hex()}")
    if torrent[b"info"].get(b"private") == 1:
        print("[warn] private torrent: DHT and PEX are disabled by the spec for these, expect 0 peers")

    # ---- Step 1: DHT ----
    print(f"\n[step 1] DHT lookup ({DHT_SECONDS}s)")
    t0 = time.time()
    dht_peers, queried, responded = dht_get_peers(info_hash)
    dht_time = time.time() - t0
    print(f"[dht] done: {len(dht_peers)} peers from {responded}/{queried} nodes in {dht_time:.1f}s")

    # ---- Step 2: PEX ----
    pex_peers, pex_stats = set(), {}
    pex_time = 0
    if dht_peers:
        print(f"\n[step 2] PEX (asking peers for their peers)")
        t0 = time.time()
        pex_peers, pex_stats = pex_discover(dht_peers, info_hash)
        pex_time = time.time() - t0
        print(f"[pex] done in {pex_time:.1f}s: {pex_stats}")

    # ---- Results ----
    all_peers = dht_peers | pex_peers
    new_from_pex = pex_peers - dht_peers
    print("\n================ RESULT ================")
    print(f"DHT peers            : {len(dht_peers)}")
    print(f"PEX peers            : {len(pex_peers)}  (new, not seen in DHT: {len(new_from_pex)})")
    print(f"Total unique peers   : {len(all_peers)}")
    print(f"Time                 : DHT {dht_time:.1f}s + PEX {pex_time:.1f}s")

    data_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
    os.makedirs(data_dir, exist_ok=True)
    for name, peers in (("dht_peers.txt", dht_peers), ("pex_peers.txt", pex_peers), ("all_peers.txt", all_peers)):
        with open(os.path.join(data_dir, name), "w") as f:
            f.write("\n".join(f"{ip}:{port}" for ip, port in sorted(peers)))
    print(f"Saved lists to       : {data_dir} (dht|pex|all)_peers.txt")

    print("\nSample peers:")
    for ip, port in sorted(all_peers)[:10]:
        print(f"  {ip}:{port}")
