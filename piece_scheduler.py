import threading


class PieceScheduler:
    """Manages piece assignment and download state."""
    
    def __init__(self, total_pieces):
        self.total_pieces = total_pieces
        self.have_pieces = set() # Successfulyl downloaded pieces
        self.in_progress = set() # currently being download
        self.peer_pieces = {}  # peer_id -> set of pieces
        self.lock = threading.Lock() # Only ONE thread can enter this code block at time
    
    def add_peer(self, peer_id, pieces):
        """Register peer's available pieces."""
        with self.lock:
            self.peer_pieces[peer_id] = pieces
    
    def next_piece(self, peer_id):
        """Get next piece to download (Lowest piece index first). 
        Future imporovement: rarest-first strategy"""
        with self.lock:
            available = self.peer_pieces.get(peer_id, set())
            
            # Find piece not downloaded and not in progress
            for piece in sorted(available):
                if piece not in self.have_pieces and piece not in self.in_progress:
                    self.in_progress.add(piece)
                    return piece
            
            return None
    
    def complete_piece(self, piece):
        """Mark piece as successfully downloaded."""
        with self.lock:
            self.in_progress.discard(piece)
            self.have_pieces.add(piece)
    
    def fail_piece(self, piece):
        """Mark piece as failed (retry later)."""
        with self.lock:
            self.in_progress.discard(piece)
    
    def progress(self):
        """Get (completed, total) tuple."""
        with self.lock:
            return (len(self.have_pieces), self.total_pieces)
    
    def is_complete(self):
        """Check if download finished."""
        with self.lock:
            return len(self.have_pieces) == self.total_pieces
