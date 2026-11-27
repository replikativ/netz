"""Transport framing for Netz draft-00.

The Netz control envelope is an ordinary deterministic CBOR item. A raw
WebSocket binding carries those bytes directly, while Kabel prefixes the item
with its four-byte, unsigned, big-endian serialization identifier. Keeping
that distinction here prevents the Kabel discriminator from accidentally
becoming part of a signed Netz object.
"""

from __future__ import annotations

from typing import Any

from netz_codec import MAX_FRAME_BYTES, dumps, loads

KABEL_CBOR_SERIALIZER_ID = 14
KABEL_PREFIX_BYTES = 4


class FramingError(ValueError):
    """A carrier frame is truncated, oversized, or uses another codec."""


def encode_websocket(value: Any) -> bytes:
    """Encode one Netz control envelope for the raw WebSocket-CBOR binding."""
    return dumps(value)


def decode_websocket(frame: bytes) -> Any:
    """Decode one raw WebSocket-CBOR control envelope."""
    if not isinstance(frame, bytes):
        raise FramingError("WebSocket binary frame must be bytes")
    if len(frame) > MAX_FRAME_BYTES:
        raise FramingError("control envelope exceeds the frame limit")
    return loads(frame)


def encode_kabel(value: Any) -> bytes:
    """Encode one control envelope using Kabel serializer id 14."""
    payload = encode_websocket(value)
    return KABEL_CBOR_SERIALIZER_ID.to_bytes(KABEL_PREFIX_BYTES, "big") + payload


def decode_kabel(frame: bytes) -> Any:
    """Decode one Kabel-CBOR frame, rejecting every other serializer id."""
    if not isinstance(frame, bytes):
        raise FramingError("Kabel binary frame must be bytes")
    if len(frame) < KABEL_PREFIX_BYTES:
        raise FramingError("truncated Kabel serializer prefix")
    if len(frame) > MAX_FRAME_BYTES + KABEL_PREFIX_BYTES:
        raise FramingError("Kabel frame exceeds the frame limit")
    serializer_id = int.from_bytes(frame[:KABEL_PREFIX_BYTES], "big")
    if serializer_id != KABEL_CBOR_SERIALIZER_ID:
        raise FramingError(f"unsupported Kabel serializer id {serializer_id}")
    return decode_websocket(frame[KABEL_PREFIX_BYTES:])


__all__ = [
    "FramingError",
    "KABEL_CBOR_SERIALIZER_ID",
    "KABEL_PREFIX_BYTES",
    "decode_kabel",
    "decode_websocket",
    "encode_kabel",
    "encode_websocket",
]
