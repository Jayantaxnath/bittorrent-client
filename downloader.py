import socket
import hashlib
from protocol import (
    create_handshake, verify_handshake, receive_bitfield,
    parse_bitfield, send_interested, wait_for_unchoke,
    send_request, wait_for_piece, recv_exact
)
import time

class DownloadWorker:
    """Manages peer downloads and coordinates with scheduler."""
    
    def __init__(self, info, info_hash, peer_id, scheduler, file_writer, total_length, peer_manager):
        self.info = info
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.scheduler = scheduler
        self.file_writer = file_writer
        self.total_length = total_length
        self.peer_manager = peer_manager
    
    def worker(self, peer):
        """Main download loop for a peer (threaded)."""
        ip, port = peer
        peer_key = f"{ip}:{port}"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        try:
            sock.settimeout(10)
            sock.connect((ip, port))
            sock.sendall(create_handshake(self.info_hash, self.peer_id))
            
            response = recv_exact(sock, 68)
            if not verify_handshake(response, self.info_hash):
                return
            
            bitfield = receive_bitfield(sock)
            self.scheduler.add_peer(peer_key, parse_bitfield(bitfield))
            
            send_interested(sock)
            if not wait_for_unchoke(sock): # Until unchoked: No downloads allowed.
                return
            
            standard_piece_length = self.info[b"piece length"]
            pieces_hashes = self.info[b"pieces"]
            
            while True:
                piece_index = self.scheduler.next_piece(peer_key)

                if piece_index is None:
                    if self.scheduler.is_complete():
                        break

                    time.sleep(1)
                    continue
                
                # Last piece may be shorter
                if piece_index == self.scheduler.total_pieces - 1:
                    piece_length = self.total_length % standard_piece_length
                    if piece_length == 0:
                        piece_length = standard_piece_length
                else:
                    piece_length = standard_piece_length
                
                # Download piece with pipelining
                if not self._download_piece(sock, piece_index, piece_length, standard_piece_length, pieces_hashes):
                    self.scheduler.fail_piece(piece_index)
                    break
        
        except Exception as e:
            print(f"[ERROR] {peer_key}: {str(e)[:60]}")
        
        finally:
            sock.close()
    
    def _download_piece(self, sock, piece_index, piece_length, standard_piece_length, pieces_hashes):
        """Download single piece with pipelining."""
        
        piece_buffer = bytearray(piece_length)
        blocks_to_request = []
        
        for offset in range(0, piece_length, 16384):
            block_size = min(16384, piece_length - offset)
            blocks_to_request.append((offset, block_size))
        
        total_blocks = len(blocks_to_request)
        blocks_received = 0
        requests_in_flight = 0 # requests_in_flight means: sent but not received
        MAX_PIPELINE = 16 # by increasing it : By the time the peer is sending the 1st block, client has already queued up requests for the 20th
        piece_failed = False
        
        while blocks_received < total_blocks:
            # Send requests until pipeline full
            while requests_in_flight < MAX_PIPELINE and blocks_to_request:
                req_offset, req_length = blocks_to_request.pop(0)
                send_request(sock, piece_index, req_offset, req_length)
                requests_in_flight += 1
            
            # Wait for response
            result = wait_for_piece(sock)
            if result is None:
                piece_failed = True
                break
            
            p_idx, begin, block = result
            
            if len(block) > 16384 or begin + len(block) > piece_length:
                piece_failed = True
                break
            
            piece_buffer[begin : begin + len(block)] = block
            blocks_received += 1
            requests_in_flight -= 1
        
        # Never trust peer: Verify and write
        if not piece_failed and blocks_received == total_blocks:
            expected_hash = pieces_hashes[piece_index * 20 : (piece_index + 1) * 20]
            actual_hash = hashlib.sha1(piece_buffer).digest()
            
            if actual_hash == expected_hash:
                absolute_offset = piece_index * standard_piece_length
                self.file_writer.write_block(absolute_offset, piece_buffer)
                self.scheduler.complete_piece(piece_index)
                
                have, total = self.scheduler.progress()
                print(f"[✓] Piece {piece_index} done. Progress: {have}/{total}")
                return True
            else:
                print(f"[✗] Piece {piece_index} hash mismatch")
                return False
        
        return False