"""Peer discovery: trackers (HTTP + UDP) and DHT. Both only PRODUCE addresses, the session decides
what to do with them. Blocking socket work (UDP tracker, DHT) stays off the event loop."""
import asyncio
import os
import random
import socket
import struct
import threading
import time
import urllib.parse

import aiohttp
import bencodepy

TRACKER_INTERVAL = 120     # re-announce so the peer pool can be refilled
DHT_LOOKUP_SECONDS = 25
DHT_PAUSE = 30             # rest between two DHT lookups
DHT_ALPHA = 16             # parallel queries per round
DHT_BOOTSTRAP = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.libtorrent.org", 25401),
]


def parse_compact_peers(data):
    """6 bytes per peer: 4 bytes ip + 2 bytes port (big-endian)."""
    peers = []
    for i in range(0, len(data) - 5, 6):
        ip = socket.inet_ntoa(data[i : i + 4])
        port = struct.unpack(">H", data[i + 4 : i + 6])[0]
        if port != 0:
            peers.append((ip, port))
    return peers


class Discovery:
    def __init__(self, session):
        self.session = session
        self.torrent = session.torrent
        self.stop_event = threading.Event()
        self.first_announce = True

    async def run(self):
        """Runs as a task inside the event loop; DHT gets its own daemon thread."""
        if not self.torrent.private:
            threading.Thread(target=self._dht_thread, args=(asyncio.get_running_loop(),), daemon=True).start()
        else:
            print("[discovery] private torrent: DHT disabled, trackers only")

        while True:
            await self._announce_all()
            self.first_announce = False
            await asyncio.sleep(TRACKER_INTERVAL)

    def stop(self):
        self.stop_event.set()

    # ------------------------------------------------------------------ trackers

    async def _announce_all(self):
        urls = self.torrent.trackers
        if not urls:
            print("[tracker] no trackers in torrent")
            return
        loop = asyncio.get_running_loop()
        async with aiohttp.ClientSession() as http:
            jobs = []
            for url in urls:
                if url.startswith("http"):
                    jobs.append(self._http_announce(http, url))
                elif url.startswith("udp://"):
                    jobs.append(self._udp_announce_async(loop, url))
            await asyncio.gather(*jobs, return_exceptions=True)

    async def _http_announce(self, http, url):
        params = {
            b"info_hash": self.torrent.info_hash,
            b"peer_id": self.session.peer_id,
            b"port": 6881,
            b"uploaded": 0,
            b"downloaded": 0,
            b"left": self.torrent.total_length,
            b"compact": 1,
        }
        if self.first_announce:
            params[b"event"] = b"started"
        try:
            async with http.get(f"{url}?{urllib.parse.urlencode(params)}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    print(f"[tracker] {url}: HTTP {resp.status}")
                    return
                reply = bencodepy.decode(await resp.read())
        except Exception as e:
            print(f"[tracker] {url}: {type(e).__name__}")
            return

        if b"failure reason" in reply or b"peers" not in reply:
            print(f"[tracker] {url}: no peers")
            return
        raw = reply[b"peers"]
        if isinstance(raw, bytes):
            peers = parse_compact_peers(raw)
        else:  # dictionary model
            peers = [(p[b"ip"].decode(), p[b"port"]) for p in raw]
        added = self.session.add_candidates(peers, priority=True)
        print(f"[tracker] {url}: {len(peers)} peers ({added} new)")

    async def _udp_announce_async(self, loop, url):
        peers = await loop.run_in_executor(None, self._udp_announce, url)
        if peers:
            added = self.session.add_candidates(peers, priority=True)
            print(f"[tracker] {url}: {len(peers)} peers ({added} new)")

    def _udp_announce(self, url):
        """BEP 15, blocking (runs in a worker thread)."""
        sock = None
        try:
            host, port = url.replace("udp://", "").split("/")[0].rsplit(":", 1)
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(8)
            addr = (host, int(port))

            tid = random.getrandbits(32)
            sock.sendto(struct.pack(">QII", 0x41727101980, 0, tid), addr)
            action, rtid, connection_id = struct.unpack(">IIQ", sock.recvfrom(16)[0])
            if action != 0 or rtid != tid:
                return None

            tid = random.getrandbits(32)
            event = 2 if self.first_announce else 0
            sock.sendto(
                struct.pack(">QII20s20sQQQIIIiH", connection_id, 1, tid, self.torrent.info_hash,
                            self.session.peer_id, 0, self.torrent.total_length, 0, event, 0, 0, 200, 6881),
                addr,
            )
            data = sock.recvfrom(4096)[0]
            action, rtid = struct.unpack(">II", data[:8])
            if action != 1 or rtid != tid or len(data) < 20:
                return None
            return parse_compact_peers(data[20:])
        except Exception as e:
            print(f"[tracker] {url}: {type(e).__name__}")
            return None
        finally:
            if sock:
                sock.close()

    # ------------------------------------------------------------------ DHT (BEP 5)

    def _dht_thread(self, loop):
        while not self.stop_event.is_set():
            found = self._dht_lookup(loop)
            print(f"[dht] lookup finished: {found} peers")
            self.stop_event.wait(DHT_PAUSE)

    def _dht_lookup(self, loop):
        """Iterative Kademlia lookup: keep asking the nodes CLOSEST (XOR distance) to the info_hash.
        A reply contains either peers ('values') or closer nodes ('nodes')."""
        info_hash = self.torrent.info_hash
        target = int.from_bytes(info_hash, "big")
        node_id = os.urandom(20)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.5)

        bootstrap = []
        for host, port in DHT_BOOTSTRAP:
            try:
                bootstrap.append((socket.gethostbyname(host), port))
            except OSError:
                pass
        candidates = {addr: 1 << 160 for addr in bootstrap}  # addr -> distance to target
        queried = set()
        total = 0
        deadline = time.time() + DHT_LOOKUP_SECONDS

        while time.time() < deadline and not self.stop_event.is_set():
            todo = sorted((a for a in candidates if a not in queried), key=candidates.get)[:DHT_ALPHA]
            if not todo:
                queried.difference_update(bootstrap)  # ask bootstrap nodes again, they answer differently
                time.sleep(1)
                continue

            for addr in todo:
                queried.add(addr)
                query = {b"t": os.urandom(2), b"y": b"q", b"q": b"get_peers",
                         b"a": {b"id": node_id, b"info_hash": info_hash}}
                try:
                    sock.sendto(bencodepy.encode(query), addr)
                except OSError:
                    pass

            round_end = time.time() + 2
            while time.time() < round_end:
                try:
                    data, _ = sock.recvfrom(65535)
                    reply = bencodepy.decode(data)
                    r = reply[b"r"]
                except Exception:
                    continue

                peers = []
                for value in r.get(b"values", []):
                    if isinstance(value, bytes):
                        peers += parse_compact_peers(value)
                if peers:
                    total += len(peers)
                    loop.call_soon_threadsafe(self.session.add_candidates, peers)

                nodes = r.get(b"nodes", b"")
                for i in range(0, len(nodes) - 25, 26):
                    nid = nodes[i : i + 20]
                    ip = socket.inet_ntoa(nodes[i + 20 : i + 24])
                    port = struct.unpack(">H", nodes[i + 24 : i + 26])[0]
                    if port:
                        candidates[(ip, port)] = int.from_bytes(nid, "big") ^ target

        sock.close()
        return total
