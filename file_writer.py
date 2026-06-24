import os
import threading


class MultiFileWriter:
    """Handles writing to single or multi-file torrents."""
    
    def __init__(self, info_dict, download_dir="."):
        self.lock = threading.Lock()
        self.files = []
        
        root_name = info_dict.get(b"name", b"download").decode('utf-8')
        base_path = os.path.join(download_dir, root_name)
        
        if b"files" in info_dict:
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
                full_path = os.path.join(base_path, *path_parts)
            else:
                full_path = os.path.join(base_path, root_name)
            
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            f_obj = open(full_path, "wb")
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
    
    def close(self):
        """Flush and close all files."""
        for f in self.files:
            f["file_obj"].flush()
            os.fsync(f["file_obj"].fileno())
            f["file_obj"].close()
        print("[I/O] Files closed and synced.")
