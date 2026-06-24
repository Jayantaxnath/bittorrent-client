# FluxTorren : Modular BitTorrent Client Architecture

## Pipeline Flow

```
Tracker Services (async)
    ↓ (raw peers)
Raw Peer Manager
    ↓ (validated peers)
Validated Peer Manager
    ↓ (stable connections)
Piece Scheduler
    ↓ (assignments)
Download Worker (threaded)
    ↓ (writes pieces)
File Writer
```

## Component Breakdown

### 1. **main.py** - Coordinator
- Loads torrent metadata
- Initializes all components
- Orchestrates pipeline execution
- Manages async/threading boundaries

### 2. **tracker_service.py** - Peer Discovery (Async)
- Extracts HTTP/HTTPS and UDP trackers independently
- Concurrent HTTP requests via aiohttp
- UDP queries in thread pool (non-blocking)
- Appends discovered peers → `raw_peer_queue`

### 3. **peer_manager.py** - Validation
- **RawPeerManager**: TCP handshake + info_hash validation (async pool)
- **ValidatedPeerManager**: Maintains stable peer list
- Deduplicated peer storage
- Dead peer removal capability

### 4. **piece_scheduler.py** - Download State
- Tracks downloaded/in-progress/available pieces
- Per-peer piece availability
- Rarest-first scheduling (sorted iteration)
- Thread-safe with locks

### 5. **downloader.py** - Download Execution
- Threaded per-peer workers
- Pipelined block requests (5 concurrent)
- SHA1 verification before disk write
- Graceful failure handling

### 6. **file_writer.py** - Disk I/O
- Single or multi-file torrent support
- Thread-safe writes across file boundaries
- Automatic fsync on close

### 7. **protocol.py** - Wire Protocol
- BitTorrent message encoding/decoding
- Handshake, bitfield, piece, request messages
- No changes from original code

---

## Key Improvements

| Issue | Solution |
|-------|----------|
| Tracker blocking | Async concurrent queries (HTTP + UDP) |
| Peer discovery slowness | Independent, non-blocking validation pool |
| Sequential downloads | Multiple threaded workers per peer |
| Monolithic flow | Clear queue-based decoupling |
| No restart capability | Components can run independently |

---

## Usage

```python
from main import CycloneClient
import asyncio

client = CycloneClient("path/to/torrent.torrent", download_dir="./downloads")
asyncio.run(client.run())
```

---

## Data Flow

1. **Tracker → Raw Queue** (IP:port tuples)
2. **Raw Manager validates** → Validated Queue
3. **Download workers query** Piece Scheduler
4. **Scheduler assigns pieces** → Worker downloads
5. **Worker writes** → File Writer (thread-safe)

---

## Threading Model

- **Main**: Async coordinator
- **Tracker queries**: Thread pool (executor)
- **Peer workers**: 1 thread per validated peer
- **File I/O**: Protected by locks

---

## Future Enhancements

- DHT peer discovery (async)
- Magnet link support
- Upload (seeding)
- Connection pooling
- Adaptive pipeline sizing
- Resume support (resume state tracking)
