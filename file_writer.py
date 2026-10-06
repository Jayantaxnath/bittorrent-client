import os
import threading


def _safe_join(base, *parts):
    """[NEW] Reject torrent paths that escape the download folder (../ or absolute paths).
    A malicious .torrent could otherwise overwrite any file on the disk."""
    base = os.path.abspath(base)
    full = os.path.abspath(os.path.join(base, *parts))
    if os.path.commonpath([base, full]) != base:
        raise ValueError(f"unsafe path in torrent: {'/'.join(parts)}")
    return full


class MultiFileWriter:
    """Handles writing to single or multi-file torrents."""

    def __init__(self, info_dict, download_dir="."):
        self.lock = threading.Lock()
        self.files = []
        self.existing_data = False  # [NEW] True if files from an earlier run were found (resume)

        root_name = info_dict.get(b"name", b"download").decode('utf-8')

        if b"files" in info_dict:
            base_path = _safe_join(download_dir, root_name)
            file_list = info_dict[b"files"]
        else:
            length = info_dict[b"length"]
            file_list = [{b"length": length, b"path": [info_dict[b"name"]]}]
            base_path = download_dir

        current_offset = 0

        for file_info in file_list:
            length = file_info[b"length"]

            if b"files" in info_dict:
                path_parts = [p.decode('utf-8') for p in file_info[b"path"]]
                full_path = _safe_join(base_path, *path_parts)
            else:
                full_path = _safe_join(base_path, root_name)

            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            # [FIX] Was open(..., "wb") which erased earlier progress on every run.
            # Now: keep an existing file ("r+b"), create only if missing ("w+b").
            # "+" also lets us READ pieces back to verify them on resume.
            if os.path.exists(full_path):
                f_obj = open(full_path, "r+b")
                if os.fstat(f_obj.fileno()).st_size > 0:
                    self.existing_data = True
            else:
                f_obj = open(full_path, "w+b")

            if os.fstat(f_obj.fileno()).st_size != length:
                f_obj.truncate(length)

            self.files.append({
                "path": full_path,
                "start": current_offset,
                "end": current_offset + length,
                "length": length,
                "file_obj": f_obj
            })

            print(f"[I/O] Mapped {full_path}")
            current_offset += length

    def write_block(self, absolute_offset, data):
        """Write block to correct file position."""
        with self.lock:
            bytes_to_write = len(data)
            current_write_offset = absolute_offset
            data_pointer = 0

            for f in self.files:
                if current_write_offset >= f["start"] and current_write_offset < f["end"]:
                    space_in_file = f["end"] - current_write_offset
                    chunk_size = min(bytes_to_write, space_in_file)
                    local_offset = current_write_offset - f["start"]

                    f["file_obj"].seek(local_offset)
                    f["file_obj"].write(data[data_pointer : data_pointer + chunk_size])

                    bytes_to_write -= chunk_size
                    current_write_offset += chunk_size
                    data_pointer += chunk_size

                    if bytes_to_write == 0:
                        break

    def read_block(self, absolute_offset, length):
        """[NEW] Read bytes back from disk (same file-boundary logic as write_block).
        Used on resume to re-hash pieces that were downloaded in a previous run."""
        with self.lock:
            data = bytearray()
            current_read_offset = absolute_offset
            bytes_to_read = length

            for f in self.files:
                if bytes_to_read == 0:
                    break
                if current_read_offset >= f["start"] and current_read_offset < f["end"]:
                    chunk_size = min(bytes_to_read, f["end"] - current_read_offset)

                    f["file_obj"].seek(current_read_offset - f["start"])
                    data.extend(f["file_obj"].read(chunk_size))

                    bytes_to_read -= chunk_size
                    current_read_offset += chunk_size

            return bytes(data)

    def close(self):
        """Flush and close all files."""
        for f in self.files:
            f["file_obj"].flush()
            os.fsync(f["file_obj"].fileno())
            f["file_obj"].close()
        print("[I/O] Files closed and synced.")
