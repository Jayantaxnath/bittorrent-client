"""Block-level piece picker.

Old designs hand out whole pieces to one peer each. Here the unit is a 16 KiB BLOCK, so:
  - many peers can work on the same piece (finish what is already started first)
  - a peer's request queue can run across piece boundaries (no idle time between pieces)
  - endgame can duplicate single blocks instead of whole pieces
Runs only inside the event loop thread, so it needs no locks.
"""
import random

BLOCK_SIZE = 16384
MAX_ACTIVE_PIECES = 64  # pieces held in RAM at the same time


class PieceState:
    """A piece that is being downloaded right now (lives in RAM until verified)."""

    __slots__ = ("index", "length", "buf", "n_blocks", "status", "received", "requested_by", "contributors")

    def __init__(self, index, length):
        self.index = index
        self.length = length
        self.buf = bytearray(length)
        self.n_blocks = (length + BLOCK_SIZE - 1) // BLOCK_SIZE
        self.status = bytearray(self.n_blocks)  # per block: 0 free, 1 requested, 2 received
        self.received = 0
        self.requested_by = {}                  # block -> set of peers that were asked for it
        self.contributors = set()               # peers that gave us data (for banning on bad hash)

    def block_length(self, b):
        return min(BLOCK_SIZE, self.length - b * BLOCK_SIZE)


class Picker:
    def __init__(self, torrent):
        self.torrent = torrent
        n = torrent.num_pieces
        self.have = bytearray(n)
        self.have_count = 0
        self.availability = [0] * n  # how many connected peers have each piece (for rarest-first)
        self.active = {}             # piece index -> PieceState

    # ---- bookkeeping ----

    def mark_have(self, index):
        if not self.have[index]:
            self.have[index] = 1
            self.have_count += 1

    def is_complete(self):
        return self.have_count == self.torrent.num_pieces

    def has_needed(self, piece_set):
        """Does this peer have at least one piece we still miss?"""
        return any(not self.have[p] for p in piece_set)

    def peer_pieces_added(self, pieces):
        for p in pieces:
            self.availability[p] += 1

    def peer_pieces_removed(self, pieces):
        for p in pieces:
            self.availability[p] -= 1

    # ---- choosing what to request ----

    def pick(self, peer, n):
        """Return up to n blocks [(piece, offset, length)] for `peer` to request now."""
        picks = []

        # 1. finish pieces that are already started (frees RAM, gets pieces verified sooner)
        for st in self.active.values():
            if len(picks) >= n:
                break
            if st.index in peer.pieces:
                self._take_free_blocks(st, peer, n, picks)

        # 2. open new pieces, RAREST first (rare pieces disappear if their few holders leave)
        while len(picks) < n and len(self.active) < MAX_ACTIVE_PIECES:
            index = self._rarest_unopened(peer.pieces)
            if index is None:
                break
            st = self.active[index] = PieceState(index, self.torrent.piece_size(index))
            self._take_free_blocks(st, peer, n, picks)

        # 3. endgame: every missing piece is already open, so idle peers ask for the same blocks
        #    a slower peer is still fetching. The first copy wins, the others get cancelled.
        if len(picks) < n and self.torrent.num_pieces - self.have_count == len(self.active):
            for st in self.active.values():
                if st.index not in peer.pieces:
                    continue
                for b in range(st.n_blocks):
                    if len(picks) >= n:
                        break
                    if st.status[b] == 1 and peer not in st.requested_by.get(b, ()):
                        st.requested_by[b].add(peer)
                        picks.append((st.index, b * BLOCK_SIZE, st.block_length(b)))

        return picks

    def _take_free_blocks(self, st, peer, n, picks):
        for b in range(st.n_blocks):
            if len(picks) >= n:
                return
            if st.status[b] == 0:
                st.status[b] = 1
                st.requested_by[b] = {peer}
                picks.append((st.index, b * BLOCK_SIZE, st.block_length(b)))

    def _rarest_unopened(self, peer_pieces):
        candidates = [p for p in peer_pieces if not self.have[p] and p not in self.active]
        if not candidates:
            return None
        # fewest holders first, random tie-break so peers don't all start the same piece
        return min(candidates, key=lambda p: (self.availability[p], random.random()))

    # ---- results ----

    def block_received(self, peer, index, begin, data):
        """Store a block. Returns (kind, other_peers_to_cancel).
        kind: "ok" | "piece_done" | "duplicate" | "stale" | "bad" """
        st = self.active.get(index)
        if st is None:
            return "stale", ()  # piece already finished or was dropped
        b = begin // BLOCK_SIZE
        if begin % BLOCK_SIZE or b >= st.n_blocks or len(data) != st.block_length(b):
            return "bad", ()
        if st.status[b] == 2:
            return "duplicate", ()

        st.buf[begin : begin + len(data)] = data
        st.status[b] = 2
        st.received += 1
        st.contributors.add(peer)
        others = [p for p in st.requested_by.pop(b, ()) if p is not peer]
        return ("piece_done" if st.received == st.n_blocks else "ok"), others

    def release(self, peer, keys):
        """Peer choked us / disconnected: its unanswered requests go back to 'free'
        (unless another peer was also asked for that block in endgame)."""
        for index, begin in keys:
            st = self.active.get(index)
            if st is None:
                continue
            b = begin // BLOCK_SIZE
            if st.status[b] == 1:
                holders = st.requested_by.get(b)
                if holders:
                    holders.discard(peer)
                if not holders:
                    st.status[b] = 0
                    st.requested_by.pop(b, None)

    def piece_done(self, index):
        self.active.pop(index, None)
        self.mark_have(index)

    def piece_failed(self, index):
        """Hash mismatch: throw the whole piece away, it will be reopened from scratch."""
        self.active.pop(index, None)
