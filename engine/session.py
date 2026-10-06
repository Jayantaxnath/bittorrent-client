"""Session = the conductor. Owns the picker, storage, peer pool and discovery.

    Discovery (trackers, DHT) -> candidates -> connector -> Peer coroutines (one per peer)
                                                               |  ask for blocks / hand in data
                                                               v
                                       Picker (block-level, rarest-first, endgame)
                                                               |  piece complete
                                                               v
                          hash pool (SHA-1 in threads) -> Storage (single disk thread) -> file

Everything except hashing and disk runs on ONE event-loop thread, so shared state needs no locks.
"""
import asyncio
import collections
import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor

from .discovery import Discovery
from .peer import Peer
from .picker import Picker
from .storage import Storage

MAX_PEERS = 60          # connected peers at once
MAX_CONNECTING = 40     # half-open connections at once (most peers on the internet are dead)
RETRY_DELAY = 20        # seconds before a peer that dropped us may be tried again
MAX_RETRIES = 2
BAN_STRIKES = 3         # bad pieces a peer may contribute to before it is banned
STALL_LIMIT = 180       # give up if no piece completes for this long
REPORT_EVERY = 2


class Stats:
    def __init__(self):
        self.downloaded = 0   # payload bytes received from peers
        self.wasted = 0       # duplicate / unrequested bytes (endgame, late blocks)
        self.hash_fails = 0
        self.peers_connected = 0
        self.peak_rate = 0.0
        self.resumed = 0
        self.peer_bytes = collections.Counter()


class Session:
    def __init__(self, torrent, out_dir, peers_file=None):
        self.torrent = torrent
        self.peer_id = b"-FX0002-" + os.urandom(12)
        self.picker = Picker(torrent)
        self.storage = Storage(torrent, out_dir)
        self.stats = Stats()
        self.hash_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="hash")
        self.discovery = Discovery(self)
        self.peers_file = peers_file

        self.candidates = collections.deque()  # addresses waiting to be tried
        self.known = set()                     # every address ever seen (no duplicates)
        self.connected = set()                 # Peer objects with a finished handshake
        self.connecting = 0
        self.banned_ips = set()
        self.attempts = {}                     # address -> how many times we retried it
        self._last_piece_time = time.monotonic()
        self.tasks = set()                     # background tasks (keeps them alive until finished)
        self.done = asyncio.Event()
        self.error = None

    # ------------------------------------------------------------------ public

    async def run(self):
        loop = asyncio.get_running_loop()
        t = self.torrent
        print(f"[torrent] {t.name}: {t.total_length / 1024 / 1024:.1f} MiB, {t.num_pieces} pieces x {t.piece_length // 1024} KiB, "
              f"{len(t.files)} file(s)")
        print(f"[torrent] info_hash {t.info_hash.hex()}")

        started = time.monotonic()
        if self.storage.existing_data:
            print("[resume] existing files found, verifying pieces on disk...")
            good = await loop.run_in_executor(None, self.storage.verify_existing)
            for index in good:
                self.picker.mark_have(index)
            self.stats.resumed = len(good)
            print(f"[resume] {len(good)}/{t.num_pieces} pieces already verified")

        if self.picker.is_complete():
            print("[done] everything is already on disk")
            self.storage.close()
            return True

        self.download_started = time.monotonic()
        self._load_peers_file()
        self.spawn(self.discovery.run())
        self.spawn(self._connector())
        self.spawn(self._reporter())

        try:
            await self.done.wait()
        finally:
            await self._shutdown()

        self._print_summary(time.monotonic() - self.download_started)
        return self.picker.is_complete()

    def add_candidates(self, peers, priority=False):
        """Called by discovery. Returns how many addresses were new."""
        new = 0
        for addr in peers:
            if addr in self.known or addr[0] in self.banned_ips:
                continue
            self.known.add(addr)
            if priority:
                self.candidates.appendleft(addr)  # tracker peers are fresher than DHT ones
            else:
                self.candidates.append(addr)
            new += 1
        return new

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    # ------------------------------------------------------------------ peer pool

    async def _connector(self):
        """Keep the pool full: start a Peer coroutine whenever there is room and a candidate."""
        while True:
            while self.candidates and self.connecting < MAX_CONNECTING and len(self.connected) < MAX_PEERS:
                addr = self.candidates.popleft()
                self.connecting += 1  # counted at spawn time, so we can never exceed MAX_CONNECTING
                self.spawn(Peer(addr, self, self.attempts.get(addr, 0)).run())
            await asyncio.sleep(0.2)

    def peer_connected(self, peer):
        self.connected.add(peer)
        self.stats.peers_connected += 1

    def peer_closed(self, peer):
        """Called by every Peer when it ends, whatever the reason."""
        self.connected.discard(peer)
        # give its unanswered requests back so other peers can fetch those blocks
        self.picker.release(peer, list(peer.outstanding))
        self.picker.peer_pieces_removed(peer.pieces)
        peer.outstanding.clear()

        # a good peer that hung up may be worth another try later
        if (peer.handshaken and peer.downloaded > 0 and peer.addr[0] not in self.banned_ips
                and peer.attempts < MAX_RETRIES and not self.done.is_set()):
            asyncio.get_running_loop().call_later(RETRY_DELAY, self._retry, peer.addr, peer.attempts + 1)

    def _retry(self, addr, attempts):
        if not self.done.is_set():
            self.candidates.append(addr)
            self.attempts[addr] = attempts

    # ------------------------------------------------------------------ pieces

    async def finish_piece(self, index):
        """All blocks of a piece arrived: verify in a thread, then write in the disk thread."""
        state = self.picker.active.get(index)
        if state is None:
            return
        loop = asyncio.get_running_loop()
        data = bytes(state.buf)
        contributors = list(state.contributors)

        digest = await loop.run_in_executor(self.hash_pool, lambda: hashlib.sha1(data).digest())
        if digest != self.torrent.piece_hashes[index]:
            self.picker.piece_failed(index)
            self.stats.hash_fails += 1
            for peer in contributors:
                peer.strikes += 1
                if peer.strikes >= BAN_STRIKES:
                    print(f"[ban] {peer} sent {peer.strikes} bad pieces")
                    self.banned_ips.add(peer.addr[0])
                    peer.kill()
            return

        try:
            await self.storage.write_piece(index, data)
        except OSError as e:
            self.error = f"disk write failed: {e}"
            self.done.set()
            return

        self.picker.piece_done(index)
        self._last_piece_time = time.monotonic()
        if self.picker.is_complete():
            self.done.set()

    # ------------------------------------------------------------------ reporting

    async def _reporter(self):
        last_have = self.picker.have_count
        samples = collections.deque()  # (time, downloaded) over the last ~6 seconds
        t = self.torrent

        while True:
            await asyncio.sleep(REPORT_EVERY)
            now = time.monotonic()
            samples.append((now, self.stats.downloaded))
            while now - samples[0][0] > 6:
                samples.popleft()
            span = now - samples[0][0]
            rate = (self.stats.downloaded - samples[0][1]) / span if span > 0 else 0
            self.stats.peak_rate = max(self.stats.peak_rate, rate)

            have = self.picker.have_count
            left = (t.num_pieces - have) * t.piece_length
            eta = f"{int(left / rate // 60)}:{int(left / rate % 60):02d}" if rate > 1000 else "--:--"
            print(f"[{have / t.num_pieces:6.1%}] {have}/{t.num_pieces} pieces | {rate / 1024:7.0f} KiB/s | "
                  f"peers {len(self.connected)} (connecting {self.connecting}, queued {len(self.candidates)}) | "
                  f"active pieces {len(self.picker.active)} | ETA {eta}")

            if have > last_have:
                last_have = have
            elif now - self._last_piece_time > STALL_LIMIT:
                self.error = f"stalled: no piece completed for {STALL_LIMIT}s"
                self.done.set()

    def _load_peers_file(self):
        if self.peers_file:
            with open(self.peers_file) as f:
                peers = [(l.split(":")[0], int(l.split(":")[1])) for l in f.read().split() if ":" in l]
            print(f"[peers] {self.add_candidates(peers)} peers loaded from {self.peers_file}")

    async def _shutdown(self):
        self.discovery.stop()
        for peer in list(self.connected):
            peer.kill()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.hash_pool.shutdown(wait=False)
        self.storage.close()

    def _print_summary(self, elapsed):
        s, t = self.stats, self.torrent
        got = (self.picker.have_count - s.resumed) * t.piece_length
        print("\n================ SUMMARY ================")
        if self.error:
            print(f"result        : FAILED ({self.error})")
        elif self.picker.is_complete():
            print("result        : COMPLETE, all pieces SHA-1 verified")
        else:
            print("result        : INTERRUPTED (progress is kept, run again to resume)")
        print(f"pieces        : {self.picker.have_count}/{t.num_pieces} ({s.resumed} came from resume)")
        print(f"time          : {elapsed:.1f} s")
        if elapsed > 0 and got > 0:
            print(f"avg speed     : {got / elapsed / 1024:.0f} KiB/s   (peak {s.peak_rate / 1024:.0f} KiB/s)")
        print(f"downloaded    : {s.downloaded / 1024 / 1024:.2f} MiB from peers, wasted {s.wasted / 1024:.0f} KiB "
              f"({s.wasted / max(s.downloaded, 1):.1%})")
        print(f"hash failures : {s.hash_fails}")
        print(f"peers         : {len(self.known)} discovered, {s.peers_connected} connected")
        for addr, nbytes in s.peer_bytes.most_common(5):
            print(f"  top peer    : {addr[0]}:{addr[1]:<5} {nbytes / 1024 / 1024:.2f} MiB")
