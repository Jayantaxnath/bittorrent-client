# FluxTorrent: A BitTorrent Client
> A hybrid BitTorrent client built to explore distributed systems, networking, concurrency, and protocol design.

![BitTorrent](https://img.shields.io/badge/Protocol-BitTorrent-green)
![Asyncio](https://img.shields.io/badge/Concurrency-Asyncio-orange)
![Multithreading](https://img.shields.io/badge/Concurrency-Multithreaded-red)
![P2P](https://img.shields.io/badge/Networking-Peer--to--Peer-purple)
![Distributed Systems](https://img.shields.io/badge/System-Distributed-blueviolet)
![Python](https://img.shields.io/badge/Python-3.10+-blue)

## Highlights

- Concurrent HTTP tracker discovery using **asyncio**
- Concurrent UDP tracker communication using **executor threads**
- Peer validation through a **50-worker handshake pool**
- Multi-peer downloading with **one thread per peer**
- Thread-safe piece scheduling using **shared state + locks**
- Pipelined block requests for improved throughput
- SHA-1 piece verification for data integrity
- Multi-file torrent support

## Architecture

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
                         DownloadWorker Threads
                           (up to ~40 threads)

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

- No duplicate downloads
- Correct piece ownership tracking
- Safe concurrent access
- Failure recovery and reassignment

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

Torrent pipelines requests:

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


## Future Improvements

- Rarest-first piece selection
- Endgame mode
- DHT support
- Magnet links
- Upload/Seeding support
- Resume downloads
- Peer Exchange (PEX)
- Fully asynchronous download engine