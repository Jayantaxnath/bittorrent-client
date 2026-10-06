import asyncio
import threading
import os
import bencodepy
import hashlib
import time
from pathlib import Path
import sys

from tracker_service import TrackerService
from peer_manager import RawPeerManager, ValidatedPeerManager
from piece_scheduler import PieceScheduler
from downloader import DownloadWorker
from file_writer import MultiFileWriter

MAX_PEERS = 40          # Maximum active download threads
NO_PEER_TIMEOUT = 120   # [NEW] give up if we have had zero connected peers for this many seconds
STATUS_INTERVAL = 5     # [NEW] seconds between progress lines


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


class FluxTorrentClient:
    def __init__(self, torrent_path, download_dir="."):
        self.torrent_data, self.info, self.info_hash, self.total_length = load_torrent(torrent_path)
        self.download_dir = download_dir
        self.peer_id = b"-FX0001-" + os.urandom(12) # 20 bytes

        # Get metadata
        self.total_pieces = len(self.info[b"pieces"]) // 20
        self.piece_length = self.info[b"piece length"]
        print(f"\n[INIT] Total pieces: {self.total_pieces}, Piece size: {self.piece_length}")

        # Initialize components
        # [FIX] download_dir was ignored before (always "."), now passed to the writer
        self.file_writer = MultiFileWriter(self.info, download_dir)
        self.piece_scheduler = PieceScheduler(self.total_pieces, self.piece_length, self.total_length)
        self.stop_event = threading.Event()  # [NEW] tells all download threads to stop

        # Producer -> Queue -> Consumer
        self.raw_peer_queue = asyncio.Queue()  # Tracker → Raw peers
        self.validated_peer_queue = asyncio.Queue()  # Validated peers

    def _resume_from_disk(self):
        """[NEW] Resume: if files from an earlier run exist, re-hash each piece on disk and
        mark the good ones as done. Never trust old data blindly, same SHA-1 check as a download."""
        if not self.file_writer.existing_data:
            return

        print("\n[resume] existing files found, verifying pieces on disk...")
        for index in range(self.total_pieces):
            data = self.file_writer.read_block(index * self.piece_length, self.piece_scheduler.piece_size(index))
            expected_hash = self.info[b"pieces"][index * 20 : (index + 1) * 20]
            if hashlib.sha1(data).digest() == expected_hash:
                self.piece_scheduler.complete_piece(index)

        have, total = self.piece_scheduler.progress()
        print(f"[resume] {have}/{total} pieces already verified")

    async def run(self):
        """Main orchestration loop."""

        self._resume_from_disk()
        if self.piece_scheduler.is_complete():
            print("[complete] all pieces already on disk, nothing to download.")
            self.file_writer.close()
            return

        # 1. Start tracker service (discovers peers, keeps re-announcing)
        tracker_task = asyncio.create_task(
            TrackerService(
                self.torrent_data,
                self.info_hash,
                self.total_length,
                self.raw_peer_queue,
                self.peer_id
            ).run()
        )

        # 2. Start raw peer manager (validates peers)
        raw_manager_task = asyncio.create_task(
            RawPeerManager(
                self.info_hash,
                self.raw_peer_queue,
                self.validated_peer_queue,
                self.peer_id
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
            validated_manager,
            self.stop_event
        )

        # [FIX] Old flow: discover for a fixed 30s, take up to 40 peers once, then download.
        # New flow: tracker, validation and downloading all run AT THE SAME TIME.
        # Every time a download thread exits (dead/bad peer), the loop below starts a new one
        # from the validated queue, so the pool stays full until the download completes.

        # Start downloads in threads (blocking I/O)
        # Threading = multiple OS threads
            # Thread 1 -> Peer A
            # Thread 2 -> Peer B
            # Thread 3 -> Peer C etc.

        print("\n[tracker] discovering peers from tcp/udp trackers...")
        download_threads = []
        last_peer_time = time.time()   # last moment we had at least one live peer
        last_status_time = time.time()
        last_have = self.piece_scheduler.progress()[0]

        try:
            while not self.piece_scheduler.is_complete():
                download_threads = [t for t in download_threads if t.is_alive()]

                if download_threads:
                    last_peer_time = time.time()
                elif time.time() - last_peer_time > NO_PEER_TIMEOUT:
                    print(f"\n[exit] no working peers for {NO_PEER_TIMEOUT}s. try again later (progress is kept).")
                    break

                if len(download_threads) < MAX_PEERS:
                    peer = await validated_manager.get_peer(timeout=1)
                    if peer:
                        t = threading.Thread(
                            target=downloader.worker,
                            args=(peer,),
                            daemon=True
                        )
                        t.start()
                        download_threads.append(t)
                else:
                    await asyncio.sleep(1)

                # [NEW] one readable status line instead of one line per piece
                now = time.time()
                if now - last_status_time >= STATUS_INTERVAL:
                    have, total = self.piece_scheduler.progress()
                    speed = (have - last_have) * self.piece_length / (now - last_status_time) / (1024 * 1024)
                    print(f"[progress] {have}/{total} pieces ({have / total:.1%}) | {len(download_threads)} peers | {speed:.2f} MB/s")
                    last_have = have
                    last_status_time = now

        finally:
            # [FIX] Always runs (done, error, or Ctrl+C): stop threads, then close files.
            # Before, files were only closed on the happy path.
            self.stop_event.set()
            tracker_task.cancel()
            raw_manager_task.cancel()

            join_deadline = time.time() + 10  # workers notice stop_event within one socket timeout
            for t in download_threads:
                t.join(timeout=max(0, join_deadline - time.time()))

            self.file_writer.close()

        # [FIX] Old code printed "100% completed" no matter what happened
        if self.piece_scheduler.is_complete():
            print("[complete] 100% download completed, all pieces verified!")
        else:
            have, total = self.piece_scheduler.progress()
            print(f"[incomplete] {have}/{total} pieces saved. run the same command again to resume.")

if __name__ == "__main__":
    # [FIX] Windows consoles/pipes often use cp1252 and crash on the ✓ / ✗ symbols we print
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    # [FIX] Self-contained service: defaults live INSIDE v1_threaded/, so it works from any folder
    service_dir = Path(__file__).resolve().parent
    torrent_path = service_dir / "torrents" / "test_folder.torrent"  # default fallback torrent
    download_dir = str(service_dir / "downloads")  # [NEW] optional 2nd argument: python main.py file.torrent ./my_dir

    if len(sys.argv) > 1:
        provided_path = sys.argv[1].replace("\\", "/")

        if provided_path and provided_path.endswith('.torrent') and os.path.isfile(provided_path):
            torrent_path = provided_path
        else:
            short_path = str(provided_path)[:20]
            print(f"Path: '{short_path}' is invalid! (Must be an existing .torrent file)")

            choice = input("Want to test with default file? (y/n): ").strip().lower()
            if choice not in ['y', 'yes']:
                print("Exiting the program.")
                sys.exit(0)

    if len(sys.argv) > 2:
        download_dir = sys.argv[2]

    # Convert the Path object back to a string for your client if needed
    print(f"[start] bittorrent client: {Path(torrent_path).resolve()}")
    print(f"[start] saving to: {os.path.abspath(download_dir)}")
    client = FluxTorrentClient(str(torrent_path), download_dir)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\n[stopped] interrupted by user, progress on disk is kept.")
