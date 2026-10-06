import threading


class PieceScheduler:
    """Manages piece assignment and download state."""

    def __init__(self, total_pieces, piece_length, total_length):
        self.total_pieces = total_pieces
        self.piece_length = piece_length
        self.total_length = total_length
        self.have_pieces = set() # Successfulyl downloaded pieces
        # [FIX] was a set. Now piece -> {peers downloading it}, so we know WHO to release
        # when a peer dies, and so endgame can give one piece to several peers.
        self.in_progress = {}
        self.peer_pieces = {}  # peer_id -> set of pieces
        # [NEW] availability[i] = how many connected peers have piece i (used for rarest-first)
        self.availability = [0] * total_pieces
        self.lock = threading.Lock() # Only ONE thread can enter this code block at time

    def piece_size(self, piece_index):
        """[NEW] Real size of a piece (the last piece may be shorter)."""
        if piece_index == self.total_pieces - 1:
            last = self.total_length % self.piece_length
            return last if last else self.piece_length
        return self.piece_length

    def add_peer(self, peer_id, pieces):
        """Register peer's available pieces."""
        with self.lock:
            # [FIX] a peer can send bitfield again: remove old counts before adding new
            self._drop_availability(peer_id)
            # [NEW] ignore out-of-range indexes sent by a broken peer
            pieces = {p for p in pieces if 0 <= p < self.total_pieces}
            self.peer_pieces[peer_id] = pieces
            for p in pieces:
                self.availability[p] += 1

    def peer_have(self, peer_id, piece):
        """[NEW] Peer sent a 'have' message: it just got a new piece."""
        with self.lock:
            pieces = self.peer_pieces.get(peer_id)
            if pieces is not None and 0 <= piece < self.total_pieces and piece not in pieces:
                pieces.add(piece)
                self.availability[piece] += 1

    def remove_peer(self, peer_id):
        """[NEW] Peer disconnected: forget its pieces AND release every piece it was downloading.
        This fixes the 'stuck piece' bug: before, a crashed worker left its piece in
        in_progress forever and nobody else could ever download it."""
        with self.lock:
            self._drop_availability(peer_id)
            for piece in list(self.in_progress):
                self._release(piece, peer_id)

    def next_piece(self, peer_id):
        """Get next piece to download.
        [FIX] Rarest-first (fewest peers have it), instead of lowest index.
        Endgame: when every remaining piece is already being downloaded, let idle
        peers download the same piece too, so one slow peer can't stall the finish."""
        with self.lock:
            available = self.peer_pieces.get(peer_id, set())

            # Find piece not downloaded and not in progress
            candidates = [
                p for p in available
                if p not in self.have_pieces and p not in self.in_progress
            ]
            if candidates:
                piece = min(candidates, key=lambda p: (self.availability[p], p))
                self.in_progress[piece] = {peer_id}
                return piece

            # [NEW] Endgame: is there any missing piece nobody has started yet?
            unstarted = [
                p for p in range(self.total_pieces)
                if p not in self.have_pieces and p not in self.in_progress and self.availability[p] > 0
            ]
            if unstarted:
                return None  # normal mode: other peers will take those, this peer has nothing to do

            duplicates = [
                p for p in available
                if p in self.in_progress and peer_id not in self.in_progress[p]
            ]
            if duplicates:
                piece = min(duplicates, key=lambda p: len(self.in_progress[p]))
                self.in_progress[piece].add(peer_id)
                return piece

            return None

    def peer_has_needed(self, peer_id):
        """[NEW] Does this peer still have any piece we are missing? If not, its thread can exit."""
        with self.lock:
            return any(p not in self.have_pieces for p in self.peer_pieces.get(peer_id, ()))

    def has_piece(self, piece):
        """[NEW] Used by endgame duplicates: stop early if another peer already finished it."""
        with self.lock:
            return piece in self.have_pieces

    def complete_piece(self, piece):
        """Mark piece as successfully downloaded."""
        with self.lock:
            self.in_progress.pop(piece, None)  # [FIX] drops all endgame duplicates too
            self.have_pieces.add(piece)

    def fail_piece(self, piece, peer_id):
        """Mark piece as failed for this peer (retry later, by this or another peer)."""
        with self.lock:
            self._release(piece, peer_id)

    def progress(self):
        """Get (completed, total) tuple."""
        with self.lock:
            return (len(self.have_pieces), self.total_pieces)

    def is_complete(self):
        """Check if download finished."""
        with self.lock:
            return len(self.have_pieces) == self.total_pieces

    # ---- helpers (caller must already hold self.lock) ----

    def _drop_availability(self, peer_id):
        for p in self.peer_pieces.pop(peer_id, set()):
            self.availability[p] -= 1

    def _release(self, piece, peer_id):
        peers = self.in_progress.get(piece)
        if peers is not None:
            peers.discard(peer_id)
            if not peers:  # nobody is downloading it any more -> free for others
                del self.in_progress[piece]
