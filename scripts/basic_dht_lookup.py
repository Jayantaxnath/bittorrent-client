"""Basic DHT lookup (first, simple version). Sequential, single-threaded.
Superseded by discover_peers.py (XOR-distance ordering, parallel queries) - kept for learning.

Usage: python scripts/basic_dht_lookup.py   (from v2_asyncio/)
"""
import os
import socket
import hashlib
import random
import bencodepy
from typing import Set, Tuple
import time

def load_torrent(path):
    """Parse .torrent file and extract info_hash, torrent data, and file size"""
    with open(path, "rb") as f:
        data = f.read()
    torrent_data = bencodepy.decode(data)
    info = torrent_data[b"info"]
    info_hash = hashlib.sha1(bencodepy.encode(info)).digest()
    if b"length" in info:
        left = info[b"length"]
    else:
        left = sum(file[b"length"] for file in info[b"files"])
    return torrent_data, info, info_hash, left


class DHTPeerDiscovery:
    def __init__(self):
        self.node_id = bytes(random.randint(0, 255) for _ in range(20))
        self.peers = set()
        self.nodes_to_query = set()
        self.queried_nodes = set()
        self.bootstrap_nodes = [
            ("router.bittorrent.com", 6881),
            ("dht.transmissionbt.com", 6881),
            ("router.utorrent.com", 6881),
        ]
    
    def _encode_bencode(self, data):
        """Bencode encoder"""
        if isinstance(data, int):
            return f"i{data}e".encode()
        elif isinstance(data, bytes):
            return f"{len(data)}:".encode() + data
        elif isinstance(data, str):
            return f"{len(data)}:{data}".encode()
        elif isinstance(data, dict):
            result = b"d"
            for k in sorted(data.keys()):
                if isinstance(k, str):
                    result += f"{len(k)}:{k}".encode()
                else:
                    result += f"{len(k)}:".encode() + k
                result += self._encode_bencode(data[k])
            result += b"e"
            return result
        elif isinstance(data, list):
            result = b"l"
            for item in data:
                result += self._encode_bencode(item)
            result += b"e"
            return result
        return b""
    
    def _parse_nodes(self, nodes_data):
        """Parse nodes response (26 bytes per node: 20 ID + 4 IP + 2 port)"""
        nodes = []
        for i in range(0, len(nodes_data) - 25, 26):
            node = nodes_data[i:i+26]
            if len(node) == 26:
                try:
                    ip = ".".join(str(b) for b in node[20:24])
                    port = int.from_bytes(node[24:26], 'big')
                    nodes.append((ip, port))
                except:
                    pass
        return nodes
    
    def _parse_peers(self, peers_data):
        """Parse peers response (6 bytes per peer: 4 IP + 2 port)"""
        peers = []
        for i in range(0, len(peers_data) - 5, 6):
            peer = peers_data[i:i+6]
            if len(peer) == 6:
                try:
                    ip = ".".join(str(b) for b in peer[0:4])
                    port = int.from_bytes(peer[4:6], 'big')
                    peers.append((ip, port))
                except:
                    pass
        return peers
    
    def query_node(self, node_addr: Tuple[str, int], info_hash: bytes, msg_type="find_node"):
        """Query a single DHT node"""
        host, port = node_addr
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)
            
            if msg_type == "find_node":
                query = {
                    b"t": b"aa",
                    b"y": b"q",
                    b"q": b"find_node",
                    b"a": {
                        b"id": self.node_id,
                        b"target": info_hash
                    }
                }
            else:  # get_peers
                query = {
                    b"t": b"aa",
                    b"y": b"q",
                    b"q": b"get_peers",
                    b"a": {
                        b"id": self.node_id,
                        b"info_hash": info_hash
                    }
                }
            
            sock.sendto(self._encode_bencode(query), (host, port))
            data, _ = sock.recvfrom(4096)
            sock.close()
            
            try:
                response = bencodepy.decode(data)
                if b"r" in response:
                    r = response[b"r"]
                    
                    # Extract peers if available
                    if b"values" in r:
                        peers = self._parse_peers(b"".join(r[b"values"]))
                        self.peers.update(peers)
                        print(f"[+] Got {len(peers)} peers from {host}:{port}")
                    
                    # Extract nodes for recursive search
                    if b"nodes" in r:
                        nodes = self._parse_nodes(r[b"nodes"])
                        for n in nodes:
                            if n not in self.queried_nodes:
                                self.nodes_to_query.add(n)
            except:
                pass
        
        except Exception as e:
            pass
    
    def discover_peers(self, info_hash: bytes, max_iterations=100):
        """Recursively discover peers by querying DHT nodes"""
        print(f"[*] Starting DHT peer discovery...")
        
        # Add bootstrap nodes
        for node in self.bootstrap_nodes:
            self.nodes_to_query.add(node)
        
        iteration = 0
        while self.nodes_to_query and iteration < max_iterations:
            node = self.nodes_to_query.pop()
            
            if node in self.queried_nodes:
                continue
            
            self.queried_nodes.add(node)
            
            # Try get_peers first (returns actual peers)
            self.query_node(node, info_hash, "get_peers")
            time.sleep(0.1)
            
            # Then find_node (returns more nodes to query)
            self.query_node(node, info_hash, "find_node")
            time.sleep(0.1)
            
            iteration += 1
            print(f"[*] Iteration {iteration}: Peers found: {len(self.peers)}, Nodes to query: {len(self.nodes_to_query)}")
        
        print(f"\n[+] Discovery complete!")
        print(f"[+] Total peers found: {len(self.peers)}")
        print(f"[+] Total nodes queried: {len(self.queried_nodes)}")
        
        return self.peers


# Usage
if __name__ == "__main__":
    torrent_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "torrents", "big-buck-bunny.torrent")) # Replace with your .torrent file path
    
    try:
        # Load torrent
        torrent_data, info, info_hash, file_size = load_torrent(torrent_file)
        print(f"[+] Torrent loaded")
        print(f"[+] Info hash: {info_hash.hex()}")
        print(f"[+] File size: {file_size} bytes\n")
        
        # Discover peers
        dht = DHTPeerDiscovery()
        peers = dht.discover_peers(info_hash, max_iterations=50)
        
        # Display results
        print(f"\n[+] Discovered Peers (first 20):")
        for ip, port in list(peers)[:20]:
            print(f"  {ip}:{port}")
    
    except Exception as e:
        print(f"[-] Error: {e}")
        import traceback
        traceback.print_exc()