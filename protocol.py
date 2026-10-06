import struct
import time

# [NEW] No legit message is bigger than a bitfield (1 bit per piece), so anything above
# this is a broken/malicious peer trying to make us allocate huge memory.
MAX_MESSAGE_LENGTH = 1024 * 1024


def recv_exact(sock, n):
    """Receive exact number of bytes."""
    data = bytearray()  # [FIX] bytearray.extend is O(1) amortized, `bytes +=` copies every time
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Peer closed connection")
        data.extend(chunk)
    return bytes(data)


def read_message(sock):
    """Read single message from peer."""
    length = struct.unpack(">I", recv_exact(sock, 4))[0]
    if length == 0:
        return ("keep_alive", None)

    # [NEW] Never trust the length prefix sent by a peer
    if length > MAX_MESSAGE_LENGTH:
        raise ConnectionError(f"Message too large ({length} bytes)")

    msg_id = recv_exact(sock, 1)[0]
    payload = recv_exact(sock, length - 1)

    return (msg_id, payload)


def create_handshake(info_hash, peer_id):
    """Create BitTorrent handshake message."""
    return bytes([19]) + b"BitTorrent protocol" + b"\x00" * 8 + info_hash + peer_id


def verify_handshake(response, info_hash):
    """Verify handshake response."""
    if len(response) != 68:  # [NEW] length guard before indexing
        return False
    if response[0] != 19 or response[1:20] != b"BitTorrent protocol":
        return False
    if response[28:48] != info_hash:
        return False
    return True


def parse_bitfield(bitfield):
    """Parse bitfield into set of piece indices."""
    pieces = set()
    if bitfield is None:
        return pieces

    for byte_idx, byte in enumerate(bitfield):
        for bit_idx in range(8):
            if byte & (1 << (7 - bit_idx)):
                pieces.add(byte_idx * 8 + bit_idx)

    return pieces


def send_interested(sock):
    """Send interested message."""
    sock.sendall(struct.pack(">IB", 1, 2))


def wait_for_unchoke(sock, on_message=None, timeout=30):
    """Wait for unchoke message.

    [FIX] Old version: (1) had no overall deadline, keep-alives could hold it forever,
    (2) threw away bitfield/have messages, so the peer looked like it had zero pieces.
    Now every other message is handed to `on_message(msg_id, payload)` so the caller
    (downloader) decides what it means. Protocol layer only reads, it doesn't decide.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg_id, payload = read_message(sock)
        if msg_id == 1:  # unchoke
            return True
        if on_message and msg_id != "keep_alive":
            on_message(msg_id, payload)  # bitfield (5), have (4), ...
    return False


def send_request(sock, piece_index, begin, block_length):
    """Send piece request."""
    msg = struct.pack(">IBIII", 13, 6, piece_index, begin, block_length)
    sock.sendall(msg)


def wait_for_piece(sock, on_message=None):
    """Wait for piece block response. Returns None if the peer chokes us."""
    while True:
        msg_id, payload = read_message(sock)
        if msg_id == 7:  # piece
            if len(payload) < 8:  # [NEW] malformed message guard
                raise ConnectionError("Malformed piece message")
            piece_index = struct.unpack(">I", payload[:4])[0]
            begin = struct.unpack(">I", payload[4:8])[0]
            return (piece_index, begin, payload[8:])
        elif msg_id == 0:  # choke
            return None
        elif on_message and msg_id != "keep_alive":
            on_message(msg_id, payload)  # [NEW] keep 'have' messages instead of dropping them
