"""
EXPERIMENT (standalone): asyncio-based BitTorrent downloader. Uses only the code in v2_asyncio/engine/.

    python download.py [torrent_file] [-o OUT_DIR] [--peers-file FILE]

Architecture: see v2_asyncio/engine/README.md, overview: v2_asyncio/README.md
"""
import argparse
import asyncio
import os
import sys

from engine.session import Session
from engine.torrent import Torrent

HERE = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="asyncio BitTorrent downloader (experiment)")
    parser.add_argument("torrent", nargs="?", default=os.path.join(HERE, "torrents", "test_folder.torrent"))
    parser.add_argument("-o", "--out", default=os.path.join(HERE, "downloads"), help="download folder (default: v2_asyncio/downloads)")
    parser.add_argument("--peers-file", help="extra peers to try first, one ip:port per line (e.g. validated_peers.txt)")
    args = parser.parse_args()

    session = Session(Torrent(args.torrent), args.out, args.peers_file)
    try:
        ok = asyncio.run(session.run())
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n[stopped] interrupted by user, progress on disk is kept")
