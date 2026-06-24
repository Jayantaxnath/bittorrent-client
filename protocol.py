import struct


def recv_exact(sock, n):
    """Receive exact number of bytes."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Peer closed connection")
        data += chunk
    return data


def read_message(sock):
    """Read single message from peer."""
    length = struct.unpack(">I", recv_exact(sock, 4))[0]
    if length == 0:
        return ("keep_alive", None)
    
    msg_id = recv_exact(sock, 1)[0]
    payload = recv_exact(sock, length - 1)
    
    return (msg_id, payload)


def create_handshake(info_hash, peer_id):
    """Create BitTorrent handshake message."""
    return bytes([19]) + b"BitTorrent protocol" + b"\x00" * 8 + info_hash + peer_id


def verify_handshake(response, info_hash):
    """Verify handshake response."""
    if response[0] != 19 or response[1:20] != b"BitTorrent protocol":
        return False
    if response[28:48] != info_hash:
        return False
    return True


def receive_bitfield(sock):
    """Receive peer's bitfield."""
    while True:
        msg_id, payload = read_message(sock)
        if msg_id == 5:  # bitfield
            return payload
        if msg_id == 1:  # choke
            return None


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


def wait_for_unchoke(sock):
    """Wait for unchoke message."""
    while True:
        msg_id, _ = read_message(sock)
        if msg_id == 1:  # unchoke
            return True


def send_request(sock, piece_index, begin, block_length):
    """Send piece request."""
    msg = struct.pack(">IBIII", 13, 6, piece_index, begin, block_length)
    sock.sendall(msg)


def wait_for_piece(sock):
    """Wait for piece block response."""
    while True:
        msg_id, payload = read_message(sock)
        if msg_id == 7:  # piece
            piece_index = struct.unpack(">I", payload[:4])[0]
            begin = struct.unpack(">I", payload[4:8])[0]
            return (piece_index, begin, payload[8:])
        elif msg_id == 0:  # choke
            return None
        elif msg_id == "keep_alive":
            continue
