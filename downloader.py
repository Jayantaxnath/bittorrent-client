import socket
import struct
import hashlib
from protocol import (
    create_handshake, verify_handshake, parse_bitfield,
    send_interested, wait_for_unchoke,
    send_request, wait_for_piece, recv_exact
)
import time

PIECE_TIMEOUT = 60  # [NEW] max seconds for one piece, so a trickling peer can't hold it forever

class DownloadWorker:
    """Manages peer downloads and coordinates with scheduler."""

    def __init__(self, info, info_hash, peer_id, scheduler, file_writer, total_length, peer_manager, stop_event):
        self.info = info
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.scheduler = scheduler
        self.file_writer = file_writer
        self.total_length = total_length
        self.peer_manager = peer_manager
        self.stop_event = stop_event  # [NEW] set by main when download ends / Ctrl+C

    def worker(self, peer):
        """Main download loop for a peer (threaded)."""
        ip, port = peer
        peer_key = f"{ip}:{port}"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # [NEW] Called for messages the protocol layer doesn't handle itself.
        # Keeps the scheduler's picture of "who has what" fresh (needed for rarest-first).
        def on_message(msg_id, payload):
            if msg_id == 5:  # bitfield
                self.scheduler.add_peer(peer_key, parse_bitfield(payload))
            elif msg_id == 4 and len(payload) == 4:  # have
                self.scheduler.peer_have(peer_key, struct.unpack(">I", payload)[0])

        try:
            sock.settimeout(10)
            sock.connect((ip, port))
            sock.sendall(create_handshake(self.info_hash, self.peer_id))

            response = recv_exact(sock, 68)
            if not verify_handshake(response, self.info_hash):
                return

            # [FIX] Register the peer first, its bitfield arrives as a normal message
            # (old receive_bitfield() could swallow the first message and lose pieces).
            self.scheduler.add_peer(peer_key, set())
            send_interested(sock)
            if not wait_for_unchoke(sock, on_message): # Until unchoked: No downloads allowed.
                return
            print(f"  [peer] {peer_key} unchoked, downloading")

            pieces_hashes = self.info[b"pieces"]

            while not self.stop_event.is_set():
                piece_index = self.scheduler.next_piece(peer_key)

                if piece_index is None:
                    if self.scheduler.is_complete():
                        break

                    # [NEW] Peer has nothing we still need -> free this thread/slot
                    if not self.scheduler.peer_has_needed(peer_key):
                        print(f"  [peer] {peer_key} has nothing we need, disconnecting")
                        break

                    time.sleep(1)
                    continue

                # Last piece may be shorter (scheduler knows the real size)
                piece_length = self.scheduler.piece_size(piece_index)

                # Download piece with pipelining
                result = self._download_piece(sock, piece_index, piece_length, pieces_hashes, on_message)
                if result == "done":
                    continue

                self.scheduler.fail_piece(piece_index, peer_key)
                if result == "choked":
                    # [FIX] Peer choked us mid-piece: wait for unchoke and go on,
                    # instead of dropping a perfectly good connection.
                    if not wait_for_unchoke(sock, on_message):
                        break
                elif result == "failed":
                    break  # bad data / timeout: don't use this peer any more
                # "skipped": another peer finished the piece first (endgame), just take the next one

        except Exception as e:
            # [FIX] type name added, socket timeouts have an empty message
            print(f"  [error] {peer_key}: {type(e).__name__} {str(e)[:50]}")

        finally:
            # [FIX] The big one: whatever way we exit (error, choke, bad hash), give back
            # this peer's pieces so other peers can download them. Before, a crash left
            # the piece marked "in progress" forever and the download hung.
            self.scheduler.remove_peer(peer_key)
            sock.close()

    def _download_piece(self, sock, piece_index, piece_length, pieces_hashes, on_message):
        """Download single piece with pipelining.
        Returns: "done" | "choked" | "failed" | "skipped" """

        piece_buffer = bytearray(piece_length)
        blocks_to_request = []

        for offset in range(0, piece_length, 16384):
            block_size = min(16384, piece_length - offset)
            blocks_to_request.append((offset, block_size))

        total_blocks = len(blocks_to_request)
        received_offsets = set() # [FIX] a peer sending the same block twice is not counted twice
        requests_in_flight = 0 # requests_in_flight means: sent but not received
        MAX_PIPELINE = 16 # by increasing it : By the time the peer is sending the 1st block, client has already queued up requests for the 20th
        deadline = time.time() + PIECE_TIMEOUT

        while len(received_offsets) < total_blocks:
            # [NEW] Stop conditions: shutting down / slow peer / endgame duplicate already finished
            if self.stop_event.is_set() or self.scheduler.has_piece(piece_index):
                return "skipped"
            if time.time() > deadline:
                print(f"  [slow] piece {piece_index} timed out")
                return "failed"

            # Send requests until pipeline full
            while requests_in_flight < MAX_PIPELINE and blocks_to_request:
                req_offset, req_length = blocks_to_request.pop(0)
                send_request(sock, piece_index, req_offset, req_length)
                requests_in_flight += 1

            # Wait for response
            result = wait_for_piece(sock, on_message)
            if result is None:
                return "choked"

            p_idx, begin, block = result

            # [FIX] late block of an earlier (skipped) piece: ignore it, it's not ours
            if p_idx != piece_index or begin in received_offsets:
                continue

            if len(block) > 16384 or begin + len(block) > piece_length:
                return "failed"

            piece_buffer[begin : begin + len(block)] = block
            received_offsets.add(begin)
            requests_in_flight -= 1

        # Never trust peer: Verify and write
        expected_hash = pieces_hashes[piece_index * 20 : (piece_index + 1) * 20]
        actual_hash = hashlib.sha1(piece_buffer).digest()

        if actual_hash == expected_hash:
            absolute_offset = piece_index * self.info[b"piece length"]
            self.file_writer.write_block(absolute_offset, piece_buffer)
            self.scheduler.complete_piece(piece_index)
            # [FIX] per-piece print removed: 40 threads flooded the terminal.
            # main.py now prints one progress line every few seconds instead.
            return "done"

        print(f"  [✗] Piece {piece_index} hash mismatch, dropping peer")
        return "failed"
