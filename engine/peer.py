"""One coroutine per peer connection (asyncio streams, no threads).

Life of a peer:  connect -> handshake -> [loop: read message -> react -> top up request queue]
The request queue is ADAPTIVE: fast peers get a deep queue (keeps the pipe full), slow peers a short one.
"""
import asyncio
import struct
import time
import traceback

from .picker import BLOCK_SIZE

CONNECT_TIMEOUT = 8       # seconds to open the TCP connection
HANDSHAKE_TIMEOUT = 8     # seconds to get the peer's handshake back
BODY_TIMEOUT = 30         # once a message header arrived, the rest must follow quickly
TICK = 2                  # wake up this often even if the peer is silent (housekeeping)
REQUEST_TIMEOUT = 25      # a request unanswered this long = peer is "snubbing" us -> drop it
CHOKE_TIMEOUT = 60        # choked for this long = not worth a slot
INACTIVITY_TIMEOUT = 120  # no message at all for this long = dead connection
KEEPALIVE_EVERY = 90
MAX_MESSAGE = 2 * 1024 * 1024

MIN_QUEUE = 10            # request queue depth for a new / slow peer
MAX_QUEUE = 150
QUEUE_SECONDS = 2.0       # keep ~2 seconds worth of data requested (bandwidth x latency)

PROTOCOL = b"BitTorrent protocol"


class ProtocolError(Exception):
    pass


class Peer:
    def __init__(self, addr, session, attempts=0):
        self.addr = addr
        self.session = session
        self.attempts = attempts

        self.reader = None
        self.writer = None
        self.handshaken = False

        self.pieces = set()       # pieces this peer has
        self.choked = True        # peer is not willing to send us data
        self.outstanding = {}     # (piece, offset) -> (length, time_sent)

        self.downloaded = 0
        self.strikes = 0          # bad pieces this peer contributed to
        self.rate = 0.0           # smoothed download speed, bytes/second
        self._bucket_bytes = 0
        self._bucket_start = time.monotonic()

        now = time.monotonic()
        self.choked_since = now
        self.last_recv = now
        self.last_send = now
        self._last_housekeeping = now

    def __repr__(self):
        return f"{self.addr[0]}:{self.addr[1]}"

    # ------------------------------------------------------------------ lifecycle

    async def run(self):
        s = self.session
        counted = True  # session.connecting was already incremented by the connector when it spawned us
        try:
            await self._connect_and_handshake()
            s.connecting -= 1
            counted = False
            self.handshaken = True
            s.peer_connected(self)
            await self._message_loop()
        except (ProtocolError, ConnectionError, OSError, asyncio.IncompleteReadError, asyncio.TimeoutError):
            pass  # normal life on the internet: peers vanish, time out, or misbehave
        except Exception:
            print(f"  [bug] unexpected error with {self}:\n{traceback.format_exc()}")
        finally:
            if counted:
                s.connecting -= 1
            if self.writer:
                self.writer.close()
            s.peer_closed(self)

    def kill(self):
        """Close the connection from outside (ban)."""
        if self.writer:
            self.writer.close()

    async def _connect_and_handshake(self):
        ip, port = self.addr
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), CONNECT_TIMEOUT
        )
        info_hash = self.session.torrent.info_hash
        self.writer.write(bytes([19]) + PROTOCOL + b"\x00" * 8 + info_hash + self.session.peer_id)

        response = await asyncio.wait_for(self.reader.readexactly(68), HANDSHAKE_TIMEOUT)
        if response[0] != 19 or response[1:20] != PROTOCOL or response[28:48] != info_hash:
            raise ProtocolError("bad handshake")

    # ------------------------------------------------------------------ main loop

    async def _message_loop(self):
        self._send(struct.pack(">IB", 1, 2))  # interested

        while True:
            self._housekeeping()

            message = await self._read_message()
            if message is None:  # quiet tick, nothing arrived
                continue

            self.last_recv = time.monotonic()
            msg_id, payload = message
            self._handle(msg_id, payload)
            self._fill_queue()

            # backpressure: don't let unsent data pile up in memory
            if self.writer.transport.get_write_buffer_size() > 256 * 1024:
                await self.writer.drain()

    async def _read_message(self):
        """Returns (msg_id, payload), (None, b'') for keep-alive, or None if nothing arrived in TICK sec.
        Only the 4-byte header read may time out quietly: cancelling it never loses data. Once the
        header is in, the body is awaited with a hard timeout that drops the peer."""
        try:
            header = await asyncio.wait_for(self.reader.readexactly(4), TICK)
        except asyncio.TimeoutError:
            return None
        length = struct.unpack(">I", header)[0]
        if length == 0:
            return (None, b"")
        if length > MAX_MESSAGE:
            raise ProtocolError("message too large")
        body = await asyncio.wait_for(self.reader.readexactly(length), BODY_TIMEOUT)
        return (body[0], body[1:])

    def _housekeeping(self):
        now = time.monotonic()
        if now - self._last_housekeeping < 1:
            return
        self._last_housekeeping = now

        if now - self.last_recv > INACTIVITY_TIMEOUT:
            raise ProtocolError("inactive")
        if self.choked and now - self.choked_since > CHOKE_TIMEOUT:
            raise ProtocolError("choked too long")
        if self.outstanding and now - min(t for _, t in self.outstanding.values()) > REQUEST_TIMEOUT:
            raise ProtocolError("snubbed")
        if now - self.last_send > KEEPALIVE_EVERY:
            self._send(b"\x00\x00\x00\x00")
        # nothing left that this peer can give us -> free the slot for a better peer
        if self.pieces and not self.outstanding and not self.session.picker.has_needed(self.pieces):
            raise ProtocolError("nothing to offer")

    # ------------------------------------------------------------------ messages

    def _handle(self, msg_id, payload):
        picker = self.session.picker
        n = self.session.torrent.num_pieces

        if msg_id is None:  # keep-alive
            return

        if msg_id == 0:  # choke: peer drops all our pending requests
            self.choked = True
            self.choked_since = time.monotonic()
            picker.release(self, list(self.outstanding))
            self.outstanding.clear()

        elif msg_id == 1:  # unchoke
            self.choked = False

        elif msg_id == 4 and len(payload) == 4:  # have
            index = struct.unpack(">I", payload)[0]
            if index < n and index not in self.pieces:
                self.pieces.add(index)
                picker.peer_pieces_added((index,))

        elif msg_id == 5:  # bitfield
            if len(payload) * 8 < n:
                raise ProtocolError("bitfield too short")
            new = {i for i in range(n) if payload[i >> 3] & (0x80 >> (i & 7))}
            picker.peer_pieces_removed(self.pieces)
            self.pieces = new
            picker.peer_pieces_added(new)

        elif msg_id == 7 and len(payload) >= 8:  # piece (a block of data)
            index, begin = struct.unpack(">II", payload[:8])
            self._on_block(index, begin, payload[8:])

        # everything else (interested, request, cancel, extended, ...) is ignored: we only leech

    def _on_block(self, index, begin, data):
        session = self.session
        request = self.outstanding.pop((index, begin), None)
        if request is None or request[0] != len(data):
            session.stats.wasted += len(data)  # unrequested / already cancelled / wrong size
            return

        self._update_rate(len(data))
        self.downloaded += len(data)
        session.stats.downloaded += len(data)
        session.stats.peer_bytes[self.addr] += len(data)

        kind, others = session.picker.block_received(self, index, begin, data)
        if kind in ("duplicate", "stale"):
            session.stats.wasted += len(data)
        elif kind == "bad":
            raise ProtocolError("bad block")
        else:
            for other in others:  # endgame: someone else was asked for this block too, cancel it
                other.cancel(index, begin, len(data))
            if kind == "piece_done":
                session.spawn(session.finish_piece(index))

    # ------------------------------------------------------------------ requests

    def _target_queue(self):
        depth = int(self.rate * QUEUE_SECONDS / BLOCK_SIZE)
        return max(MIN_QUEUE, min(MAX_QUEUE, depth))

    def _fill_queue(self):
        """Keep the request queue full. Blocks come from ANY piece (pipelining across pieces)."""
        if self.choked or not self.pieces:
            return
        want = self._target_queue() - len(self.outstanding)
        if want <= 0:
            return

        now = time.monotonic()
        batch = []
        for index, offset, length in self.session.picker.pick(self, want):
            self.outstanding[(index, offset)] = (length, now)
            batch.append(struct.pack(">IBIII", 13, 6, index, offset, length))
        if batch:
            self._send(b"".join(batch))  # one write for the whole batch

    def cancel(self, index, begin, length):
        if self.outstanding.pop((index, begin), None) is not None:
            self._send(struct.pack(">IBIII", 13, 8, index, begin, length))

    def _send(self, data):
        if not self.writer.is_closing():
            self.writer.write(data)
            self.last_send = time.monotonic()

    def _update_rate(self, nbytes):
        self._bucket_bytes += nbytes
        now = time.monotonic()
        elapsed = now - self._bucket_start
        if elapsed >= 1.0:
            instant = self._bucket_bytes / elapsed
            self.rate = instant if self.rate == 0 else 0.6 * self.rate + 0.4 * instant
            self._bucket_bytes = 0
            self._bucket_start = now
