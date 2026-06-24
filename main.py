import asyncio
import threading
import os
import bencodepy
import hashlib
from pathlib import Path
import sys

from tracker_service import TrackerService
from peer_manager import RawPeerManager, ValidatedPeerManager
from piece_scheduler import PieceScheduler
from downloader import DownloadWorker
from file_writer import MultiFileWriter


def load_torrent(path):
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


class CycloneClient:
    def __init__(self, torrent_path, download_dir="."):
        self.torrent_data, self.info, self.info_hash, self.total_length = load_torrent(torrent_path)
        self.download_dir = download_dir
        self.peer_id = b"-CY0001-" + os.urandom(12) # 20 bytes
        
        # Get metadata
        self.total_pieces = len(self.info[b"pieces"]) // 20
        self.piece_length = self.info[b"piece length"]
        print(f"\n[INIT] Total pieces: {self.total_pieces}, Piece size: {self.piece_length}")
        
        # Initialize components
        self.file_writer = MultiFileWriter(self.info, download_dir)
        self.piece_scheduler = PieceScheduler(self.total_pieces)
        
        # Producer -> Queue -> Consumer
        self.raw_peer_queue = asyncio.Queue()  # Tracker → Raw peers
        self.validated_peer_queue = asyncio.Queue()  # Validated peers

    async def run(self):
        """Main orchestration loop."""
        
        # 1. Start tracker service (discovers peers)
        tracker_task = asyncio.create_task(
            TrackerService(
                self.torrent_data, 
                self.info_hash, 
                self.total_length, 
                self.raw_peer_queue
            ).run()
        )
        
        # 2. Start raw peer manager (validates peers)
        raw_manager_task = asyncio.create_task(
            RawPeerManager(
                self.info_hash,
                self.raw_peer_queue,
                self.validated_peer_queue
            ).run()
        )
        
        # 3. Manage validated peers and coordinate downloads
        validated_manager = ValidatedPeerManager(self.validated_peer_queue)
        
        # 4. Start download workers (consume piece assignments)
        downloader = DownloadWorker(
            self.info,
            self.info_hash,
            self.peer_id,
            self.piece_scheduler,
            self.file_writer,
            self.total_length,
            validated_manager
        )
        
        # Run trackers + peer validation for N seconds
        print("[START] Discovering peers...")
        try:
            # Tracker -> raw_peer_queue -> RawPeerManager -> validated_peer_queue : is continuously working for 20s
            await asyncio.wait_for(
                asyncio.gather(tracker_task, raw_manager_task),
                timeout=30
            )
        except asyncio.TimeoutError:
            print("[INFO] Peer discovery timeout, starting downloads...")
            tracker_task.cancel()
            raw_manager_task.cancel()
        
        # Get validated peers
        peers = await validated_manager.get_all_peers()
        print(f"[READY] {len(peers)} validated peers. Starting download...")
        
        # Start downloads in threads (blocking I/O)
        # Threading = multiple OS threads
            # Thread 1 -> Peer A
            # Thread 2 -> Peer B
            # Thread 3 -> Peer C etc.

        download_threads = []
        for peer_info in peers:

            t = threading.Thread(
                target=downloader.worker,
                args=(peer_info,),
                daemon=True
            )
            t.start()
            download_threads.append(t)

        # Why not asyncio for downloading?
        
        # Wait for completion
        for t in download_threads:
            t.join()
        
        self.file_writer.close()
        print("[COMPLETE] Download finished!")

if __name__ == "__main__":
    # Default fallback path
    torrent_path = Path("./torrent_files/sintel.torrent")

    if len(sys.argv) > 1:
        provided_path = sys.argv[1].replace("\\", "/")
        
        if provided_path and provided_path.endswith('.torrent'):
            torrent_path = provided_path
        else:
            short_path = str(provided_path)[:20]
            print(f"Path: '{short_path}' is invalid! (Must end in .torrent)")

            choice = input("Want to test with default file? (y/n): ").strip().lower()
            if choice not in ['y', 'yes']:
                print("Exiting the program.")
                sys.exit(0)

    # Convert the Path object back to a string for your client if needed
    print(f"Running client with: {torrent_path}")
    client = CycloneClient(str(torrent_path))
    asyncio.run(client.run())