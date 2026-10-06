"""Torrent metadata: everything we need to know about the .torrent file, in one object."""
import hashlib
import bencodepy


class Torrent:
    def __init__(self, path):
        with open(path, "rb") as f:
            meta = bencodepy.decode(f.read())
        info = meta[b"info"]

        self.info_hash = hashlib.sha1(bencodepy.encode(info)).digest()
        self.name = info[b"name"].decode("utf-8", "replace")
        self.piece_length = info[b"piece length"]
        self.private = info.get(b"private") == 1

        pieces = info[b"pieces"]
        self.piece_hashes = [pieces[i : i + 20] for i in range(0, len(pieces), 20)]
        self.num_pieces = len(self.piece_hashes)

        # files = [(path_parts, length)], in the order they are laid out on the "virtual disk"
        self.multi_file = b"files" in info
        if self.multi_file:
            self.files = [
                ([p.decode("utf-8", "replace") for p in f[b"path"]], f[b"length"])
                for f in info[b"files"]
            ]
        else:
            self.files = [([self.name], info[b"length"])]
        self.total_length = sum(length for _, length in self.files)

        # tracker urls (announce + announce-list), duplicates removed
        trackers = []
        if b"announce" in meta:
            trackers.append(meta[b"announce"].decode("utf-8", "replace"))
        for tier in meta.get(b"announce-list", []):
            for url in tier:
                trackers.append(url.decode("utf-8", "replace"))
        self.trackers = list(dict.fromkeys(trackers))

    def piece_size(self, index):
        """Real size of a piece (the last piece may be shorter)."""
        if index == self.num_pieces - 1:
            last = self.total_length % self.piece_length
            return last if last else self.piece_length
        return self.piece_length
