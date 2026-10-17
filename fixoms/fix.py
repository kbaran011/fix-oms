"""Minimal FIX 4.4 tag=value encoding and decoding."""
import re
from datetime import datetime, timezone

SOH = "\x01"
BEGIN_STRING = "FIX.4.4"
_HEADER = re.compile(rb"8=FIX[^\x01]*\x019=(\d+)\x01")


class FixError(Exception):
    pass


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def checksum(data: bytes) -> int:
    return sum(data) % 256


def encode(msg_type, sender, target, seq, body=(), poss_dup=False, orig_time=None, sending_time=None) -> bytes:
    fields = [(35, msg_type), (49, sender), (56, target), (34, seq), (52, sending_time or utc_now())]
    if poss_dup:
        fields.append((43, "Y"))
        if orig_time:
            fields.append((122, orig_time))
    fields.extend(body)
    payload = "".join(f"{tag}={value}{SOH}" for tag, value in fields).encode()
    msg = f"8={BEGIN_STRING}{SOH}9={len(payload)}{SOH}".encode() + payload
    return msg + f"10={checksum(msg):03d}{SOH}".encode()


def decode(raw: bytes) -> dict:
    """Parse one message into {tag: value}. Repeating groups are not supported (last value wins)."""
    header = _HEADER.match(raw)
    if not header:
        raise FixError("bad header")
    trailer = raw.rfind(b"\x0110=")
    if trailer < 0 or not raw.endswith(SOH.encode()):
        raise FixError("missing checksum")
    if trailer + 1 - header.end() != int(header.group(1)):
        raise FixError("body length mismatch")
    if checksum(raw[:trailer + 1]) != int(raw[trailer + 4:-1]):
        raise FixError("checksum mismatch")
    msg = {}
    for field in raw[:trailer].decode().split(SOH):
        tag, _, value = field.partition("=")
        msg[int(tag)] = value
    return msg


def split_messages(buf: bytes):
    """Split a TCP byte stream into complete messages using BodyLength. Returns (messages, leftover)."""
    msgs = []
    while True:
        start = buf.find(b"8=FIX")
        if start < 0:
            return msgs, buf[-4:]  # keep a possible partial "8=FI"
        buf = buf[start:]
        header = _HEADER.match(buf)
        if not header:
            if SOH.encode() in buf[:32] and len(buf) > 32:
                buf = buf[1:]  # garbage that happens to start with 8=FIX, resync
                continue
            return msgs, buf
        end = header.end() + int(header.group(1)) + len("10=000\x01")
        if len(buf) < end:
            return msgs, buf
        msgs.append(buf[:end])
        buf = buf[end:]


def pretty(raw: bytes) -> str:
    return raw.decode(errors="replace").replace(SOH, "|")
