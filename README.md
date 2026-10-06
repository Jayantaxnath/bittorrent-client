# FluxTorrent v1 (`v1_threaded/`): threaded BitTorrent client
> A hybrid BitTorrent client built to explore distributed systems, networking, concurrency, and protocol design.

<!-- ![BitTorrent](https://img.shields.io/badge/Protocol-BitTorrent-green)
![Asyncio](https://img.shields.io/badge/Concurrency-Asyncio-orange)
![Multithreading](https://img.shields.io/badge/Concurrency-Multithreaded-red)
![P2P](https://img.shields.io/badge/Networking-Peer--to--Peer-purple)
![Distributed Systems](https://img.shields.io/badge/System-Distributed-blueviolet)
![Python](https://img.shields.io/badge/Python-3.10+-blue) -->

## Highlights

- Concurrent HTTP tracker discovery using **asyncio**
- Concurrent UDP tracker communication using **executor threads**
- Peer validation through a **50-worker handshake pool**
- Multi-peer downloading with **one thread per peer**, refilled continuously as peers die
- Thread-safe piece scheduling using **shared state + locks**
- **Rarest-first** piece selection and **endgame mode**
- Pipelined block requests for improved throughput
- SHA-1 piece verification for data integrity
- **Resume support**: existing pieces are re-verified and skipped
- Multi-file torrent support (with path-traversal protection)

## Architecture

Full concurrency diagram: [docs/concurrency-architecture.png](docs/concurrency-architecture.png), details in [ARCHITECTURE.md](ARCHITECTURE.md).

```
            HTTP/HTTPS Trackers             UDP Trackers
                    │                            │
                    ▼                            ▼
        ┌───────────────────────┐    ┌───────────────────────┐
        │ TrackerService (HTTP) │    │ TrackerService (UDP)  │
        │   (Asyncio Tasks)     │    │  (Executor Threads)   │
        └───────────┬───────────┘    └───────────┬───────────┘
                    │                            │
                    └──────────────┬─────────────┘
                                   │
                                   ▼
                             raw_peer_queue
                             (asyncio.Queue)
                                   │
                                   ▼
                        ┌─────────────────────┐
                        │   RawPeerManager    │
                        │ (Thread Pool - 50)  │
                        └──────────┬──────────┘
                                   │
                                   ▼
                         validated_peer_queue
                           (asyncio.Queue)
                                   │
                                   ▼
                         Download pool (main.py)
                    DownloadWorker Threads (up to 40,
                       replaced when a peer dies)

           Peer A      Peer B      Peer C    ...     Peer X
             │           │           │                 │
             └───────────┴─────┬─────┴─────────────────┘
                               │
                               ▼
                   ┌───────────────────────┐
                   │    PieceScheduler     │
                   │    (Shared State)     │
                   │   [threading.Lock]    │
                   └───────────┬───────────┘
                               │
                               ▼
                   ┌───────────────────────┐
                   │      FileWriter       │
                   │     (Thread-safe)     │
                   └───────────┬───────────┘
                               │
                               ▼
                        Downloaded File
```

## Concurrency Model

### Async Layer

Used for tracker discovery and orchestration.

```text
HTTP Tracker 1
HTTP Tracker 2
HTTP Tracker 3
        │
        ▼
 asyncio.gather(...)
```

### Validation Layer

Peer handshakes execute concurrently inside a thread pool.

```text
Thread 1  -> Peer A
Thread 2  -> Peer B
Thread 3  -> Peer C
...
Thread 50 -> Peer Z
```

### Download Layer

Each validated peer receives a dedicated download thread.

```text
Thread 1 -> Peer A
Thread 2 -> Peer B
Thread 3 -> Peer C
...
```

## Piece Scheduling

Shared state maintained across all download threads:

```python
have_pieces
in_progress
peer_pieces
```

The scheduler guarantees:

- No duplicate downloads (except on purpose in endgame)
- Correct piece ownership tracking
- Safe concurrent access
- Failure recovery: a dead peer's pieces are released for others
- Rarest-first: the piece the fewest peers have is downloaded first

Implemented using:

```python
threading.Lock()
```


## Request Pipelining

Instead of:

```text
Request
Wait
Request
Wait
Request
Wait
```

FluxTorrent pipelines requests:

```text
Request 1
Request 2
Request 3
...
Request 16
```

allowing peers to continuously stream blocks without idle network time.


## Piece Verification

Every completed piece is verified before writing:

```text
Downloaded Piece
        │
        ▼
SHA-1(piece)
        │
        ▼
Expected Torrent Hash
```

Only verified pieces are committed to disk.

## Core Concepts Demonstrated

- Distributed Systems
- Peer-to-Peer Networking
- Async Programming
- Multithreading
- Thread Synchronization
- Producer-Consumer Architecture
- Queue-Based Communication
- TCP/UDP Socket Programming
- Binary Protocol Implementation
- Data Integrity Verification


## Usage

```bash
cd v1_threaded
pip install -r requirements.txt
python main.py                                 # default: torrents/test_folder.torrent -> downloads/
python main.py torrents/test_folder.torrent [download_dir]
```

The second argument (download folder) is optional, default is `v1_threaded/downloads/`. Run the same command again to resume.

## Future Improvements

- DHT support
- Magnet links
- Upload/Seeding support
- Peer Exchange (PEX)
- Pipelining across piece boundaries
- Fully asynchronous download engine