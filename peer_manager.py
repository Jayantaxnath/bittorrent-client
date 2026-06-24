import asyncio
import socket
import struct
from protocol import (
    create_handshake, verify_handshake, 
    receive_bitfield, parse_bitfield, recv_exact
)


class RawPeerManager:
    """Validates raw peers via TCP handshake + info_hash check."""
    
    # most OS limits to about 1024 open sockets by default
    def __init__(self, info_hash, raw_queue, validated_queue, max_concurrent=50):
        self.info_hash = info_hash
        self.raw_queue = raw_queue
        self.validated_queue = validated_queue
        self.max_concurrent = max_concurrent
        self.validated = set()
    
    async def run(self):
        """Process raw peers and validate them."""
        tasks = set()
        
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
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # fix 1
            for task in done:
                try:
                    peer_info = task.result()
                    if peer_info and peer_info not in self.validated: # Only first one survives, if there's duplicate peers
                        self.validated.add(peer_info)
                        await self.validated_queue.put(peer_info)
                except Exception as e:
                    print(f"  [QUEUE ERROR] {repr(e)}")

    async def _validate_peer(self, peer):
        """TCP connect + Handshake + info_hash verification (blocking in executor)."""
        loop = asyncio.get_event_loop()
        # using thread because socket has blocking scoket calls like sock.connect(...) and recv_exact(...)
        return await loop.run_in_executor(None, self._blocking_handshake, peer)
    
    def _blocking_handshake(self, peer):
        """Synchronous handshake validation."""
        ip, port = peer
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((ip, port))
            
            peer_id = b"-CY0001-" + b"0" * 12
            sock.sendall(create_handshake(self.info_hash, peer_id))
            
            response = recv_exact(sock, 68)
            sock.close()
            
            if verify_handshake(response, self.info_hash):
                print(f"  ✓ Validated peer {ip}:{port}")
                return (ip, port) # Return a tuple, NOT a dictionary!
            
        except Exception as e:
            print(f"[HANDSHAKE ERROR] {e}")
        
        return None


class ValidatedPeerManager:
    """Maintains stable peer pool."""
    
    def __init__(self, validated_queue):
        self.validated_queue = validated_queue
        self.peers = []
        self.lock = asyncio.Lock()
    
    async def get_all_peers(self):
        """Wait for peer accumulation then return list."""
        print("[PEERS] Waiting for validated peers...")
        
        # Collect peers for a time
        # Modern CPUs can easily juggle 40 concurrent I/O-bound Python th
        while len(self.peers) < 40:  # Maximum active download threads
            try:
                peer = await asyncio.wait_for(self.validated_queue.get(), timeout=5)
                if peer:
                    self.peers.append(peer)
            except asyncio.TimeoutError:
                break
        
        return self.peers
    
    async def get_peer(self):
        """Get next available peer (for future load-balancing)."""
        async with self.lock:
            if self.peers:
                return self.peers.pop(0)
        return None
    
    async def mark_peer_inactive(self, peer):
        """Remove dead peer from pool."""
        async with self.lock:
            self.peers = [p for p in self.peers if p != peer]
