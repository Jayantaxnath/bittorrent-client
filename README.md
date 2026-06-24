# BitTorrent Client in Python

> A hybrid BitTorrent client built to explore distributed systems, networking, concurrency, and protocol design.

## Highlights

- Concurrent HTTP tracker discovery using **asyncio**
- Concurrent UDP tracker communication using **executor threads**
- Peer validation through a **50-worker handshake pool**
- Multi-peer downloading with **one thread per peer**
- Thread-safe piece scheduling using **shared state + locks**
- Pipelined block requests for improved throughput
- SHA-1 piece verification for data integrity
- Multi-file torrent support

## TCP  Protocol

```
  ┌─────────┐                                     ┌─────────┐
  │ Client  │                                     │ Server  │
  └────┬────┘                                     └────┬────┘
       │                                               │
       │                                               │
       │ ──────────────── SYN (Seq=0) ───────────────▶ │  Step 1:
       │                                               │  Connection
       │ ◀──────── SYN-ACK (Seq=0, Ack=1) ──────────── │  Establishment
       │                                               │  (3-Way Handshake)
       │ ──────────────── ACK (Ack=1) ───────────────▶ │
       │                                               │
       ├───────────────────────────────────────────────┤
       │                                               │
       │ ────────── Data (Seq=1, Len=100) ───────────▶ │  Step 2:
       │                                               │  Reliable Data
       │ ◀────────────── ACK (Ack=101) ─────────────── │  Transfer
       │                                               │
       ├───────────────────────────────────────────────┤
       │                                               │
       │ ────────────────── FIN ─────────────────────▶ │  Step 3:
       │                                               │  Connection
       │ ◀───────────────── ACK ────────────────────── │  Termination
       │                                               │  (4-Way Teardown)
       │ ◀───────────────── FIN ────────────────────── │
       │                                               │
       │ ────────────────── ACK ─────────────────────▶ │
       ▼                                               ▼
```

## UDP Protocol

```
  ┌─────────┐                                     ┌─────────┐
  │ Client  │                                     │ Server  │
  └────┬────┘                                     └────┬────┘
       │                                               │
       │                                               │
       │ ────────────── Data Datagram 1 ─────────────▶ │  No Handshake.
       │                                               │  Data is sent
       │ ────────────── Data Datagram 2 ─────────────▶ │  immediately.
       │                                               │
       │ ─ ─ ─ ─ ─ ─ ─ Data Datagram 3 (Lost) ─ ─ ─ ─ ─|   No Acknowledgment.
       │                                               │  No Retransmission.
       │ ────────────── Data Datagram 4 ─────────────▶ │
       │                                               │
       ▼                                               ▼
```

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