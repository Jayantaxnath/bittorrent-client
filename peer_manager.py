import asyncio
import socket
from concurrent.futures import ThreadPoolExecutor
from protocol import create_handshake, verify_handshake, recv_exact


class RawPeerManager:
    """Validates raw peers via TCP handshake + info_hash check.
    Why validate before downloading? Most tracker peers are dead/unreachable. Testing 50 at a
    time here (5s timeout) keeps the 40 download threads for peers that actually answer."""

    # most OS limits to about 1024 open sockets by default
    def __init__(self, info_hash, raw_queue, validated_queue, peer_id, max_concurrent=50):
        self.info_hash = info_hash
        self.raw_queue = raw_queue
        self.validated_queue = validated_queue
        self.peer_id = peer_id  # [FIX] real client id (was a fake all-zeros id)
        self.max_concurrent = max_concurrent
        self.validated = set()
        # [FIX] Own thread pool. The default executor only has ~cpu+4 threads (and is shared
        # with UDP trackers), so "50 concurrent validators" was really ~12.
        self.pool = ThreadPoolExecutor(max_workers=max_concurrent)

    async def run(self):
        """Process raw peers and validate them."""
        tasks = set()

        try:
            while True:
                # Keep pool full
                while len(tasks) < self.max_concurrent: # Keep 50 validators busy
                    try:
                        peer = self.raw_queue.get_nowait() # Fill pool quickly, no waiting
                        task = asyncio.create_task(self._validate_peer(peer))
                        tasks.add(task)
                    except asyncio.QueueEmpty:
                        break

                if not tasks:
                    await asyncio.sleep(0.1)
                    continue

                # Wait for any to complete
                # [FIX] timeout added: new peers from the tracker are picked up even while
                # all running validations are slow (previously waited for a task to finish).
                done, tasks = await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED)

                # fix 1
                for task in done:
                    try:
                        peer_info = task.result()
                        if peer_info and peer_info not in self.validated: # Only first one survives, if there's duplicate peers
                            self.validated.add(peer_info)
                            await self.validated_queue.put(peer_info)
                    except Exception as e:
                        print(f"  [error] {repr(e)}")
        finally:
            # [NEW] stop the handshake threads when main cancels this task
            self.pool.shutdown(wait=False, cancel_futures=True)

    async def _validate_peer(self, peer):
        """TCP connect + Handshake + info_hash verification (blocking in executor)."""
        loop = asyncio.get_running_loop()
        # using thread because socket has blocking scoket calls like sock.connect(...) and recv_exact(...)
        return await loop.run_in_executor(self.pool, self._blocking_handshake, peer)

    def _blocking_handshake(self, peer):
        """Synchronous handshake validation."""
        ip, port = peer
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.settimeout(5)
            sock.connect((ip, port))

            sock.sendall(create_handshake(self.info_hash, self.peer_id))

            response = recv_exact(sock, 68)

            if verify_handshake(response, self.info_hash):
                print(f"  [peer] ✓ {ip}:{port} answered handshake")
                return (ip, port) # Return a tuple, NOT a dictionary!

        except Exception as e:
            # print(f"[HANDSHAKE ERROR] {e}")
            pass

        finally:
            sock.close()  # [FIX] socket was leaked when connect/recv raised

        return None


class ValidatedPeerManager:
    """Hands validated peers to the download pool in main.py, one at a time.
    [FIX] Old version collected 40 peers once and returned them. Now main.py asks for a
    peer whenever a download thread finishes, so dead peers get replaced continuously."""

    def __init__(self, validated_queue):
        self.validated_queue = validated_queue

    async def get_peer(self, timeout=1):
        """Get next validated peer, or None if none arrived within `timeout` seconds."""
        try:
            return await asyncio.wait_for(self.validated_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
