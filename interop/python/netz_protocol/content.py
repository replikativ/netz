"""Strict portable single-block content discovery and transfer.

Profile 1 intentionally supports only CIDv1/raw/SHA-256.  The restriction is
on content addressing, not on block contents: ``data`` is an opaque byte
string and may contain CBOR, encrypted Konserve bytes, Git objects, or anything
else an application chooses.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Iterable

from netz_codec import dumps, loads

CONTENT_CAPABILITY = "netz/content/1"
CID_PREFIX = bytes((0x01, 0x55, 0x12, 0x20))
CID_SIZE = 36
REQUEST_SIZE = 16
AUTHORITY_SIZE = 32
MAX_ANNOUNCED_CIDS = 256
MAX_PROVIDERS = 64
MAX_BLOCK_BYTES = 1024 * 1024
MAX_PROVIDER_TTL_MS = 20 * 60 * 1000


class ContentError(ValueError):
    """A portable content frame or CID is invalid."""


def raw_cid(data: bytes) -> bytes:
    """Return the profile-1 CIDv1 for opaque ``data``."""
    if not isinstance(data, bytes):
        raise ContentError("block data must be bytes")
    if len(data) > MAX_BLOCK_BYTES:
        raise ContentError("block exceeds the profile limit")
    return CID_PREFIX + sha256(data).digest()


def validate_cid(value: object) -> bytes:
    if (not isinstance(value, bytes) or len(value) != CID_SIZE
            or not value.startswith(CID_PREFIX)):
        raise ContentError("CID must be CIDv1/raw/SHA-256")
    return value


def verify_block(cid: object, data: object) -> bool:
    """Whether opaque block bytes exactly match a profile-1 CID."""
    try:
        return isinstance(data, bytes) and raw_cid(data) == validate_cid(cid)
    except ContentError:
        return False


def _request(value: object) -> bytes:
    if not isinstance(value, bytes) or len(value) != REQUEST_SIZE:
        raise ContentError("request must be exactly 16 bytes")
    return value


def _authority(value: object) -> bytes:
    if not isinstance(value, bytes) or len(value) != AUTHORITY_SIZE:
        raise ContentError("provider must be a full 32-byte Authority ID")
    return value


def _ordered_unique(values: list[bytes]) -> bool:
    return all(left < right for left, right in zip(values, values[1:]))


def announce_frame(cids: Iterable[bytes]) -> dict[str, object]:
    """Construct a canonical, bounded provider announcement."""
    canonical = sorted(set(validate_cid(cid) for cid in cids))
    if len(canonical) > MAX_ANNOUNCED_CIDS:
        raise ContentError("too many announced CIDs")
    return {"v": 1, "type": "content/announce", "cids": canonical}


def find_frame(request: bytes, cid: bytes) -> dict[str, object]:
    return {"v": 1, "type": "content/find",
            "request": _request(request), "cid": validate_cid(cid)}


def found_frame(request: bytes, cid: bytes, providers: Iterable[bytes], *,
                ttl_ms: int) -> dict[str, object]:
    canonical = sorted(set(_authority(provider) for provider in providers))
    if len(canonical) > MAX_PROVIDERS:
        raise ContentError("too many providers")
    if (type(ttl_ms) is not int
            or not 0 <= ttl_ms <= MAX_PROVIDER_TTL_MS):
        raise ContentError("provider TTL is outside the profile limit")
    return {"v": 1, "type": "content/found", "request": _request(request),
            "cid": validate_cid(cid), "providers": canonical, "ttl_ms": ttl_ms}


def want_frame(request: bytes, cid: bytes) -> dict[str, object]:
    return {"v": 1, "type": "content/want",
            "request": _request(request), "cid": validate_cid(cid)}


def block_frame(request: bytes, cid: bytes, data: bytes) -> dict[str, object]:
    if not verify_block(cid, data):
        raise ContentError("block data does not match CID")
    return {"v": 1, "type": "content/block", "request": _request(request),
            "cid": validate_cid(cid), "data": data}


def dont_have_frame(request: bytes, cid: bytes) -> dict[str, object]:
    return {"v": 1, "type": "content/dont-have",
            "request": _request(request), "cid": validate_cid(cid)}


def validate_frame(value: object) -> dict[str, object]:
    """Validate one exact profile-1 content map without normalizing it."""
    if (not isinstance(value, dict) or type(value.get("v")) is not int
            or value.get("v") != 1):
        raise ContentError("invalid content frame")
    frame_type = value.get("type")
    common = {"v", "type", "request", "cid"}
    if frame_type == "content/announce":
        cids = value.get("cids")
        if (set(value) != {"v", "type", "cids"}
                or not isinstance(cids, list)
                or len(cids) > MAX_ANNOUNCED_CIDS):
            raise ContentError("invalid content announcement")
        for cid in cids:
            validate_cid(cid)
        if not _ordered_unique(cids):
            raise ContentError("announced CIDs are not canonical")
    elif frame_type in {"content/find", "content/want", "content/dont-have"}:
        if set(value) != common:
            raise ContentError("invalid content request schema")
        _request(value.get("request"))
        validate_cid(value.get("cid"))
    elif frame_type == "content/found":
        providers = value.get("providers")
        ttl = value.get("ttl_ms")
        if (set(value) != common | {"providers", "ttl_ms"}
                or not isinstance(providers, list)
                or len(providers) > MAX_PROVIDERS):
            raise ContentError("invalid content provider response")
        _request(value.get("request"))
        validate_cid(value.get("cid"))
        for provider in providers:
            _authority(provider)
        if not _ordered_unique(providers):
            raise ContentError("providers are not canonical")
        if type(ttl) is not int or not 0 <= ttl <= MAX_PROVIDER_TTL_MS:
            raise ContentError("provider TTL is outside the profile limit")
    elif frame_type == "content/block":
        if set(value) != common | {"data"}:
            raise ContentError("invalid content block schema")
        _request(value.get("request"))
        if not verify_block(value.get("cid"), value.get("data")):
            raise ContentError("block data does not match CID")
    else:
        raise ContentError("unknown content frame type")
    return dict(value)


def encode_frame(value: object) -> bytes:
    return dumps(validate_frame(value))


def decode_frame(encoded: bytes) -> dict[str, object]:
    try:
        return validate_frame(loads(encoded))
    except ContentError:
        raise
    except Exception as error:
        raise ContentError("invalid content encoding") from error
