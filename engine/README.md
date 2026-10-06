# FluxTorrent v2 engine: asyncio BitTorrent downloader

Standalone, lives only in `v2_asyncio/`. Run (from v2_asyncio/): `python download.py [torrent] [-o OUT_DIR] [--peers-file FILE]`

## Flow

```
Discovery (trackers + DHT)
      | addresses
      v
Session.candidates --> connector (max 60 peers, max 40 half-open connects)
                              |
                              v
                Peer coroutine x N  (asyncio, one per connection)
                  read message -> react -> top up request queue
                              |  ask for blocks / hand in data
                              v
              Picker (block-level: rarest-first, cross-piece pipeline, endgame)
                              |  piece complete (all blocks in RAM)
                              v
          hash pool (SHA-1 in 4 threads) -> Storage (ONE disk thread) -> files
```

## Files

| File | Job |
|---|---|
| `torrent.py` | Parse .torrent: info_hash, pieces, files, trackers |
| `discovery.py` | Tracker (HTTP/UDP) and DHT peer sources, they only produce addresses |
| `peer.py` | One peer connection: handshake, message loop, adaptive request queue, timeouts |
| `picker.py` | Decides which BLOCK each peer requests next |
| `session.py` | Conductor: peer pool, retries, bans, hashing, progress, summary |
| `storage.py` | Disk thread, multi-file mapping, resume verification, path safety |

## Design decisions (interview answers)

1. **asyncio, not thread-per-peer.** Network work is waiting; one event loop handles hundreds of
   sockets with no locks (picker and session state are only touched by the loop thread).
   Only two things leave the loop: SHA-1 (thread pool, hashlib releases the GIL) and disk (one thread).
2. **Block-level picker.** Unit of work is a 16 KiB block, not a piece. Several peers can fill the same
   piece, and a peer's queue continues into the next piece, so the connection never idles.
3. **Adaptive queue depth.** Depth = speed x 2 s / 16 KiB (min 10, max 150): fast peers keep the pipe full,
   slow peers don't hoard blocks.
4. **Rarest-first + endgame.** Rare pieces are fetched before their few holders leave. When every missing
   piece is already open, idle peers request the same blocks; the first copy wins and the rest are cancelled.
5. **Peer hygiene.** Requests unanswered for 25 s = snubbed and dropped; peers choked for 60 s are dropped;
   a peer that contributed to 3 bad pieces is banned; good peers that hang up are retried twice.
6. **One disk thread.** Files are only touched by one thread, writes happen only after SHA-1 passes,
   existing pieces are re-verified on start (resume), torrent paths cannot escape the download folder.

## Known limits

- Leech only: no upload, no incoming connections (peers that need tit-for-tat may choke us).
- IPv4 only, plain TCP (no uTP, no encryption), no PEX/magnet inside the engine.
- Up to 64 pieces are buffered in RAM at once (`MAX_ACTIVE_PIECES`), fine for small pieces, heavy for 4 MiB+ pieces.
