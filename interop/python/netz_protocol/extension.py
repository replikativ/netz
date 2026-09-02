"""Portable capability-gated extension envelopes for the Kabel binding."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from netz_codec import CBORError, MAX_FRAME_BYTES, dumps, loads

EXTENSION_CARRIER_TYPE = "netz/extension-cbor/1"
EXTENSION_CARRIER_CAPABILITY = EXTENSION_CARRIER_TYPE
EXTENSION_ENVELOPE_TYPE = "netz/extension"
WIRE_PROFILE_CAPABILITY = "wire/netz-cbor/1"
KABEL_PREFIX_BYTES = 4
MAX_KABEL_PAYLOAD_BYTES = MAX_FRAME_BYTES - KABEL_PREFIX_BYTES


class ExtensionError(ValueError):
    """An extension capability, carrier, or deterministic envelope is invalid."""


def _valid_capability(value: object) -> bool:
    return (isinstance(value, str)
            and 1 <= len(value) <= 128
            and all(0x21 <= ord(char) <= 0x7e for char in value))


def encode_extension_envelope(capability: str, payload: Any) -> bytes:
    """Return the canonical inner envelope shared by all implementations."""
    if not _valid_capability(capability):
        raise ExtensionError("invalid extension capability")
    try:
        return dumps({
            "v": 1,
            "type": EXTENSION_ENVELOPE_TYPE,
            "capability": capability,
            "payload": payload,
        })
    except (CBORError, TypeError, ValueError) as error:
        raise ExtensionError("extension payload is outside wire profile 1") from error


def decode_extension_envelope(encoded: bytes) -> tuple[str, Any]:
    """Validate canonical inner bytes and return ``(capability, payload)``."""
    if not isinstance(encoded, bytes):
        raise ExtensionError("extension envelope must be bytes")
    try:
        envelope = loads(encoded)
    except (CBORError, TypeError, ValueError) as error:
        raise ExtensionError("invalid deterministic extension envelope") from error
    if (not isinstance(envelope, dict)
            or set(envelope) != {"v", "type", "capability", "payload"}
            or envelope.get("v") != 1
            or envelope.get("type") != EXTENSION_ENVELOPE_TYPE
            or not _valid_capability(envelope.get("capability"))):
        raise ExtensionError("invalid extension envelope fields")
    return envelope["capability"], envelope["payload"]


def _head(major: int, value: int) -> bytes:
    """Encode one canonical CBOR head without the profile's 1 MiB string cap."""
    if value < 24:
        return bytes([(major << 5) | value])
    if value <= 0xff:
        return bytes([(major << 5) | 24, value])
    if value <= 0xffff:
        return bytes([(major << 5) | 25]) + value.to_bytes(2, "big")
    if value <= 0xffffffff:
        return bytes([(major << 5) | 26]) + value.to_bytes(4, "big")
    return bytes([(major << 5) | 27]) + value.to_bytes(8, "big")


def _require_negotiated(capability: str,
                        selected_capabilities: Collection[str]) -> None:
    if (not isinstance(selected_capabilities, Collection)
            or isinstance(selected_capabilities, (str, bytes))
            or not {WIRE_PROFILE_CAPABILITY,
                    EXTENSION_CARRIER_CAPABILITY,
                    capability}.issubset(set(selected_capabilities))):
        raise ExtensionError("extension carrier capabilities were not negotiated")


def encode_extension_frame(capability: str, payload: Any, *,
                           selected_capabilities: Collection[str]) -> bytes:
    """Return the exact CBOR payload accepted by ``SecureWebSocket.send_frame``."""
    _require_negotiated(capability, selected_capabilities)
    inner = encode_extension_envelope(capability, payload)
    marker = EXTENSION_CARRIER_TYPE.encode("ascii")
    frame = b"\x82" + _head(3, len(marker)) + marker + _head(2, len(inner)) + inner
    if len(frame) > MAX_KABEL_PAYLOAD_BYTES:
        raise ExtensionError("extension carrier exceeds the Kabel frame limit")
    return frame


def _argument(frame: bytes, offset: int, additional: int) -> tuple[int | None, int]:
    if additional < 24:
        return additional, offset
    sizes = {24: 1, 25: 2, 26: 4, 27: 8}
    if additional == 31:
        return None, offset
    size = sizes.get(additional)
    if size is None or offset + size > len(frame):
        raise ExtensionError("invalid extension carrier length")
    return int.from_bytes(frame[offset:offset + size], "big"), offset + size


def _string_item(frame: bytes, offset: int, major: int,
                 limit: int) -> tuple[bytes, int]:
    if offset >= len(frame):
        raise ExtensionError("truncated extension carrier")
    initial = frame[offset]
    offset += 1
    if initial >> 5 != major:
        raise ExtensionError("invalid extension carrier item")
    length, offset = _argument(frame, offset, initial & 31)
    if length is not None:
        if length > limit or offset + length > len(frame):
            raise ExtensionError("extension carrier item exceeds its limit")
        return frame[offset:offset + length], offset + length

    chunks = bytearray()
    while True:
        if offset >= len(frame):
            raise ExtensionError("unterminated extension carrier item")
        if frame[offset] == 0xff:
            return bytes(chunks), offset + 1
        if frame[offset] & 31 == 31:
            raise ExtensionError("nested indefinite carrier strings are invalid")
        chunk, offset = _string_item(frame, offset, major, limit - len(chunks))
        chunks.extend(chunk)


def _decode_carrier(frame: bytes) -> bytes:
    """Decode Kabel's semantic outer value; only the inner map is canonical."""
    if not isinstance(frame, bytes) or len(frame) > MAX_KABEL_PAYLOAD_BYTES:
        raise ExtensionError("invalid extension carrier size")
    if not frame or frame[0] >> 5 != 4:
        raise ExtensionError("extension carrier must be an array")
    length, offset = _argument(frame, 1, frame[0] & 31)
    if length not in (2, None):
        raise ExtensionError("extension carrier must contain two items")
    marker_bytes, offset = _string_item(frame, offset, 3, 128)
    try:
        marker = marker_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ExtensionError("invalid extension carrier marker") from error
    inner, offset = _string_item(frame, offset, 2, MAX_KABEL_PAYLOAD_BYTES)
    if length is None:
        if offset >= len(frame) or frame[offset] != 0xff:
            raise ExtensionError("unterminated extension carrier array")
        offset += 1
    if offset != len(frame) or marker != EXTENSION_CARRIER_TYPE:
        raise ExtensionError("invalid extension carrier")
    return inner


def decode_extension_frame(frame: bytes, *,
                           selected_capabilities: Collection[str]) -> tuple[str, Any]:
    """Decode one serializer-14 payload and enforce transcript capabilities."""
    capability, payload = decode_extension_envelope(_decode_carrier(frame))
    _require_negotiated(capability, selected_capabilities)
    return capability, payload


__all__ = [
    "EXTENSION_CARRIER_CAPABILITY",
    "EXTENSION_CARRIER_TYPE",
    "EXTENSION_ENVELOPE_TYPE",
    "ExtensionError",
    "WIRE_PROFILE_CAPABILITY",
    "decode_extension_envelope",
    "decode_extension_frame",
    "encode_extension_envelope",
    "encode_extension_frame",
]
