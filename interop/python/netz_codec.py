"""Dependency-free codec for the Netz draft-00 control-envelope CBOR subset.

This is a conformance aid, not yet a network client. It deliberately rejects
floats, tags, indefinite lengths and non-text map keys in protocol envelopes.
Opaque application byte strings may contain richer CBOR; their application
profile, not this control codec, defines how they are decoded.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MAX_FRAME_BYTES = 5 * 1024 * 1024
MAX_STRING_BYTES = 1024 * 1024
MAX_ITEMS = 16384
MAX_DEPTH = 64


class CBORError(ValueError):
    """Invalid or non-profile CBOR."""


def _head(major: int, value: int) -> bytes:
    if value < 0:
        raise CBORError("negative CBOR argument")
    prefix = major << 5
    if value < 24:
        return bytes([prefix | value])
    if value <= 0xFF:
        return bytes([prefix | 24, value])
    if value <= 0xFFFF:
        return bytes([prefix | 25]) + value.to_bytes(2, "big")
    if value <= 0xFFFFFFFF:
        return bytes([prefix | 26]) + value.to_bytes(4, "big")
    if value <= 0xFFFFFFFFFFFFFFFF:
        return bytes([prefix | 27]) + value.to_bytes(8, "big")
    raise CBORError("integer outside uint64 range")


def _dumps(value: Any, depth: int, items: list[int]) -> bytes:
    if depth > MAX_DEPTH:
        raise CBORError("maximum nesting exceeded")
    items[0] += 1
    if items[0] > MAX_ITEMS:
        raise CBORError("maximum item count exceeded")
    if value is None:
        return b"\xf6"
    if value is False:
        return b"\xf4"
    if value is True:
        return b"\xf5"
    if isinstance(value, int):
        if not -(1 << 63) <= value < (1 << 63):
            raise CBORError("integer outside signed 64-bit range")
        if value >= 0:
            return _head(0, value)
        return _head(1, -1 - value)
    if isinstance(value, bytes):
        if len(value) > MAX_STRING_BYTES:
            raise CBORError("maximum string size exceeded")
        return _head(2, len(value)) + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        if len(encoded) > MAX_STRING_BYTES:
            raise CBORError("maximum string size exceeded")
        return _head(3, len(encoded)) + encoded
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_ITEMS - items[0]:
            raise CBORError("maximum item count exceeded")
        return _head(4, len(value)) + b"".join(
            _dumps(item, depth + 1, items) for item in value)
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise CBORError("profile-1 map keys must be text")
        if len(value) * 2 > MAX_ITEMS - items[0]:
            raise CBORError("maximum item count exceeded")
        pairs = [(_dumps(key, depth + 1, items),
                  _dumps(item, depth + 1, items))
                 for key, item in value.items()]
        pairs.sort(key=lambda pair: pair[0])
        return _head(5, len(pairs)) + b"".join(k + v for k, v in pairs)
    raise CBORError(f"unsupported profile-1 value: {type(value).__name__}")


def dumps(value: Any) -> bytes:
    """Encode one bounded value using RFC 8949 deterministic CBOR."""
    encoded = _dumps(value, 0, [0])
    if len(encoded) > MAX_FRAME_BYTES:
        raise CBORError("maximum frame size exceeded")
    return encoded


@dataclass
class _Reader:
    data: bytes
    offset: int = 0
    items: int = 0

    def take(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.data):
            raise CBORError("truncated CBOR")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def argument(self, additional: int) -> int:
        if additional < 24:
            return additional
        sizes = {24: 1, 25: 2, 26: 4, 27: 8}
        if additional not in sizes:
            raise CBORError("indefinite or reserved additional information")
        raw = self.take(sizes[additional])
        value = int.from_bytes(raw, "big")
        minimum = {24: 24, 25: 0x100, 26: 0x10000, 27: 0x100000000}[additional]
        if value < minimum:
            raise CBORError("non-minimal integer/length encoding")
        return value

    def item(self, depth: int = 0) -> Any:
        if depth > MAX_DEPTH:
            raise CBORError("maximum nesting exceeded")
        self.items += 1
        if self.items > MAX_ITEMS:
            raise CBORError("maximum item count exceeded")
        initial = self.take(1)[0]
        major, additional = initial >> 5, initial & 31
        if major in (0, 1):
            value = self.argument(additional)
            return value if major == 0 else -1 - value
        if major in (2, 3):
            size = self.argument(additional)
            if size > MAX_STRING_BYTES:
                raise CBORError("maximum string size exceeded")
            raw = self.take(size)
            if major == 2:
                return raw
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError as error:
                raise CBORError("invalid UTF-8") from error
        if major == 4:
            size = self.argument(additional)
            if size > MAX_ITEMS - self.items:
                raise CBORError("maximum item count exceeded")
            return [self.item(depth + 1) for _ in range(size)]
        if major == 5:
            size = self.argument(additional)
            if size > (MAX_ITEMS - self.items) // 2:
                raise CBORError("maximum item count exceeded")
            result: dict[str, Any] = {}
            previous: bytes | None = None
            for _ in range(size):
                start = self.offset
                key = self.item(depth + 1)
                encoded_key = self.data[start:self.offset]
                if not isinstance(key, str):
                    raise CBORError("profile-1 map keys must be text")
                if previous is not None and encoded_key <= previous:
                    raise CBORError("map keys are duplicate or non-deterministic")
                previous = encoded_key
                result[key] = self.item(depth + 1)
            return result
        if major == 7 and additional in (20, 21, 22):
            return {20: False, 21: True, 22: None}[additional]
        raise CBORError("value is outside the profile-1 subset")


def loads(data: bytes) -> Any:
    """Decode one deterministic profile-1 item and reject trailing bytes."""
    if len(data) > MAX_FRAME_BYTES:
        raise CBORError("maximum frame size exceeded")
    reader = _Reader(bytes(data))
    value = reader.item()
    if reader.offset != len(reader.data):
        raise CBORError("trailing bytes")
    if dumps(value) != data:
        raise CBORError("input was valid CBOR but not deterministic profile-1")
    return value
