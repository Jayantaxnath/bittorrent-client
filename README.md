# FluxTorrent v2 (`v2_asyncio/`): asyncio download engine + DHT/PEX experiments

A **self-contained service**: it has its own code, sample torrents, download folder and dependencies.
It imports nothing from `v1_threaded/` and can be copied anywhere and run on its own.

## Folder layout

```
v2_asyncio/
├── download.py            ← ENTRY POINT: download a torrent with the asyncio engine
├── requirements.txt       aiohttp, bencodepy
├── engine/                the asyncio downloader (see engine/README.md for the design)
│   ├── torrent.py         parse .torrent
│   ├── discovery.py       trackers (HTTP/UDP) + DHT
│   ├── peer.py            one peer connection (coroutine)
│   ├── picker.py          block-level piece picker (rarest-first, endgame)
│   ├── session.py         peer pool, hashing, progress, summary
│   └── storage.py         disk thread, resume, safe paths
├── scripts/               step-by-step experiments, run them in this order
│   ├── basic_dht_lookup.py    1. first simple DHT lookup (learning version)
│   ├── discover_peers.py      2. DHT + PEX: find peers without a tracker
│   └── validate_peers.py      3. check which of those peers we can REALLY download from
├── torrents/              sample .torrent files (test_folder, big-buck-bunny)
├── data/                  generated peer lists (dht_peers, pex_peers, all_peers, validated_peers)
└── downloads/             default output folder of download.py
```

## The story: from "find peers" to "download"

```
1 basic_dht_lookup   simple, sequential DHT walk (slow, but easy to read)
        ↓
2 discover_peers     proper Kademlia lookup (closest-first by XOR distance, 16 queries in parallel)
                     + PEX (ask connected peers for THEIR peers)         → data/all_peers.txt
        ↓
3 validate_peers     handshake → has pieces → unchoked → sent a real block → data/validated_peers.txt
        ↓
4 download.py        asyncio engine downloads, using trackers + DHT (+ optional validated peers)
```

## How to run (from inside `v2_asyncio/`)

```bash
cd v2_asyncio
pip install -r requirements.txt

# 1-3: discovery experiments (default torrent: torrents/big-buck-bunny.torrent, a popular one with many peers)
python scripts/discover_peers.py [torrent_file]
python scripts/validate_peers.py [torrent_file] [peers_file]

# 4: download (default torrent: torrents/test_folder.torrent, default output: downloads/)
python download.py
python download.py torrents/test_folder.torrent -o ./my_downloads
python download.py torrents/big-buck-bunny.torrent --peers-file data/validated_peers.txt
```

Run it again on the same folder to **resume**: finished pieces are re-verified and skipped.
Requirements: Python 3.10+.

## Results so far (one live swarm each, numbers vary between runs)

| Experiment | Result |
|---|---|
| DHT + PEX on big-buck-bunny | 430 peers from DHT (30 s), PEX added 6 (3 new) |
| Validation of those 433 peers | 33 handshakes OK, **20 downloadable** (all seeders), 91% never answered |
| v1 (`../v1_threaded`) on test_folder | 83-90 s, ~0.22 MiB/s (2 runs) |
| v2 engine on test_folder | **20-39 s, 0.5-0.95 MiB/s** (4 runs), 0 hash failures, resume works |

## Notes and limits

- The engine is **leech only** (no upload, no incoming connections), IPv4 only, no PEX inside the engine.
- Private torrents: DHT/PEX are disabled by the spec, so discovery uses trackers only.
- DHT peer lists are mostly stale: expect ~5% of the peers to be usable.
