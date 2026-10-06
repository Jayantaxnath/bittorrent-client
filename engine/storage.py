"""Disk layer.

All file access goes through ONE dedicated disk thread, so the asyncio event loop never blocks
on disk and we never write from two threads at once. Pieces are written only after SHA-1 passes.
"""
import asyncio
import hashlib
import os
import threading
from concurrent.futures import ThreadPoolExecutor


def safe_join(base, *parts):
    """Reject torrent paths that escape the download folder (../ or absolute paths)."""
    base = os.path.abspath(base)
    full = os.path.abspath(os.path.join(base, *parts))
    if os.path.commonpath([base, full]) != base:
        raise ValueError(f"unsafe path in torrent: {'/'.join(parts)}")
    return full


class Storage:
    def __init__(self, torrent, out_dir):
        self.torrent = torrent
        self.lock = threading.Lock()
        self.disk = ThreadPoolExecutor(max_workers=1, thread_name_prefix="disk")
        self.existing_data = False  # True if files from an earlier run were found
        self.files = []             # (start, end, path, file_object)

        offset = 0
        for parts, length in torrent.files:
            if torrent.multi_file:
                path = safe_join(out_dir, torrent.name, *parts)
            else:
                path = safe_join(out_dir, *parts)
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # keep an existing file ("r+b") so we can resume, create only if missing ("w+b")
            if os.path.exists(path):
                f = open(path, "r+b")
                if os.fstat(f.fileno()).st_size > 0:
                    self.existing_data = True
            else:
                f = open(path, "w+b")
            if os.fstat(f.fileno()).st_size != length:
                f.truncate(length)

            self.files.append((offset, offset + length, path, f))
            offset += length

    # ---- low level (called only with self.lock held or from the disk thread) ----

    def _write(self, offset, data):
        view = memoryview(data)
        with self.lock:
            for start, end, _, f in self.files:
                if not view:
                    break
                if start <= offset < end:
                    n = min(len(view), end - offset)
                    f.seek(offset - start)
                    f.write(view[:n])
                    view = view[n:]
                    offset += n

    def _read(self, offset, length):
        out = bytearray()
        with self.lock:
            for start, end, _, f in self.files:
                if length == 0:
                    break
                if start <= offset < end:
                    n = min(length, end - offset)
                    f.seek(offset - start)
                    out += f.read(n)
                    offset += n
                    length -= n
        return bytes(out)

    # ---- public API ----

    async def write_piece(self, index, data):
        """Write a verified piece without blocking the event loop."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.disk, self._write, index * self.torrent.piece_length, data)

    def verify_existing(self):
        """Resume: re-hash every piece already on disk, return the set of good piece indexes.
        (blocking, call it through run_in_executor)"""
        t = self.torrent

        def check(i):
            data = self._read(i * t.piece_length, t.piece_size(i))
            return i if hashlib.sha1(data).digest() == t.piece_hashes[i] else None

        with ThreadPoolExecutor(max_workers=4) as pool:
            return {i for i in pool.map(check, range(t.num_pieces)) if i is not None}

    def close(self):
        self.disk.shutdown(wait=True)  # finish queued writes first
        for _, _, _, f in self.files:
            f.flush()
            os.fsync(f.fileno())
            f.close()
