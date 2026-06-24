import asyncio
import aiohttp
import struct
import socket
import bencodepy
from typing import Set, Tuple
import urllib.parse  # Put this at the top of tracker_service.py


class TrackerService:
    """Discovers peers from HTTP/HTTPS and UDP trackers concurrently."""
    
    def __init__(self, torrent_data, info_hash, left, peer_queue):
        self.torrent_data = torrent_data
        self.info_hash = info_hash
        self.left = left
        self.peer_queue = peer_queue
        self.peer_id = b"-CY0001-" + b"0" * 12  # Placeholder
        
        self.discovered = set()
    
    async def run(self):
        """Main entry: extract trackers and query concurrently."""
        tracker_urls = self._extract_trackers()
        
        if not tracker_urls:
            print("[TRACKERS] No trackers found")
            return
        
        print(f"[TRACKERS] Found {len(tracker_urls)} trackers")
        
        # Query HTTP and UDP in parallel
        await asyncio.gather(
            self._query_http_trackers([t for t in tracker_urls if t.startswith("http")]),
            self._query_udp_trackers([t for t in tracker_urls if t.startswith("udp://")]),
            return_exceptions=True
        )
    
    def _extract_trackers(self) -> Set[str]:
        """Extract tracker URLs from torrent metadata."""
        trackers = set()
        
        if b"announce" in self.torrent_data:
            trackers.add(self.torrent_data[b"announce"].decode())
        
        if b"announce-list" in self.torrent_data:
            for tier in self.torrent_data[b"announce-list"]:
                for url in tier:
                    trackers.add(url.decode())
        
        return trackers
    
    async def _query_http_trackers(self, urls: list):
        """Query HTTP/HTTPS trackers concurrently."""
        if not urls:
            return
        
        async with aiohttp.ClientSession() as session:
            tasks = [self._http_announce(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True) # switch point 1, 2 is in _http_announce
        
        for peers in results:
            if peers:
                for peer in peers:
                    await self.peer_queue.put(peer) # switch point 3

    async def _http_announce(self, session, tracker_url):
        """Single HTTP tracker query."""
        try:
            # Use byte-keys and byte-values for proper URL encoding
            query_string = urllib.parse.urlencode({
                b"info_hash": self.info_hash,
                b"peer_id": self.peer_id,
                b"port": 6881,
                b"uploaded": 0,
                b"downloaded": 0,
                b"left": self.left,
                b"compact": 1,
            })
            
            # Append the properly encoded string to the URL manually
            url_with_query = f"{tracker_url}?{query_string}"
            
            # Do NOT use the params= kwarg here anymore
            async with session.get(url_with_query, timeout=aiohttp.ClientTimeout(5)) as resp:
                if resp.status != 200:
                    print(f"  [HTTP {resp.status}] {tracker_url}")
                    return None
                
                content = await resp.read() # switch point 2
                tracker_response = bencodepy.decode(content)
                
                if b"failure reason" in tracker_response:
                    print(f"  [Rejected] {tracker_url}")
                    return None
                
                if b"peers" not in tracker_response:
                    return None
                
                peers_data = tracker_response[b"peers"]
                peers = self._parse_peers(peers_data)
                
                if peers:
                    print(f"  ✓ {tracker_url}: {len(peers)} peers")
                    return peers
        
        except asyncio.TimeoutError:
            print(f"  [Timeout] {tracker_url}")
        except Exception as e:
            print(f"  [Error] {tracker_url}: {str(e)[:40]}")
        
        return None
    
    async def _query_udp_trackers(self, urls: list):
        """Query UDP trackers in thread pool."""
        if not urls:
            return
        
        # socket and sock.recvfrom is blocking so using loop.run_in_executor
        # to assigns urls works to multiple thread
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(None, self._udp_announce, url)
            for url in urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for peers in results:
            if peers:
                for peer in peers:
                    await self.peer_queue.put(peer)
    
    def _udp_announce(self, tracker_url):
        """Single UDP tracker query (blocking)."""
        try:
            url_parts = tracker_url.replace("udp://", "").split("/")[0]
            host, port = url_parts.rsplit(":", 1)
            port = int(port)
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # blocking 1
            sock.settimeout(5)
            
            # Connect
            protocol_id = 0x41727101980
            action = 0
            transaction_id = 12345
            connect_req = struct.pack(">QII", protocol_id, action, transaction_id)
            sock.sendto(connect_req, (host, port))
            
            response, _ = sock.recvfrom(16) # blocking 2
            action_resp, trans_id_resp, connection_id = struct.unpack(">IIQ", response)
            
            if trans_id_resp != transaction_id or action_resp != 0:
                sock.close()
                return None
            
            # Announce
            action = 1
            transaction_id = 12346
            announce_req = struct.pack(
                ">QII20s20sQQQIIIIH",
                connection_id, action, transaction_id,
                self.info_hash, self.peer_id,
                0, self.left, 0,  # downloaded, left, uploaded
                0, 0, 0, 100, 6881  # event, ip, key, num_want, port
            )
            sock.sendto(announce_req, (host, port))
            
            response, _ = sock.recvfrom(4096)
            if len(response) < 20:
                sock.close()
                return None
            
            action_resp, trans_id_resp, interval, leechers, seeders = struct.unpack(">IIIII", response[:20])
            if trans_id_resp != transaction_id or action_resp != 1:
                sock.close()
                return None
            
            # Parse peers
            peers = []
            peer_data = response[20:]
            for i in range(0, len(peer_data), 6):
                if i + 6 <= len(peer_data):
                    ip = ".".join(map(str, peer_data[i:i+4]))
                    port = struct.unpack(">H", peer_data[i+4:i+6])[0]
                    peers.append((ip, port))
            
            sock.close()
            
            if peers:
                print(f"  ✓ {tracker_url}: {len(peers)} peers")
            return peers if peers else None
        
        except Exception as e:
            print(f"  [UDP Error] {tracker_url}: {str(e)[:40]}")
            return None
    
    def _parse_peers(self, peers_data):
        """Parse compact peer format."""
        peers = []
        if isinstance(peers_data, bytes):
            for i in range(0, len(peers_data), 6):
                if i + 6 <= len(peers_data):
                    ip = ".".join(map(str, peers_data[i:i+4]))
                    port = struct.unpack(">H", peers_data[i+4:i+6])[0]
                    peers.append((ip, port))
        else:
            for peer in peers_data:
                ip = peer[b"ip"].decode() if isinstance(peer[b"ip"], bytes) else peer[b"ip"]
                port = peer[b"port"]
                peers.append((ip, port))
        
        return peers
