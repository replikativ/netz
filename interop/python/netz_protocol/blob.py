"""Portable immutable-blob descriptor for resumable Netz replication."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, Callable, Collection

from netz_codec import MAX_STRING_BYTES, dumps, is_profile_version

OBJECT_TAG = b"netz/blob-object/v1\x00"
PIECE_TAG = b"netz/blob-piece/v1\x00"
MANIFEST_TAG = b"netz/blob-manifest/v1\x00"
DEFAULT_PIECE_SIZE = 65_536
MAX_PIECES = 16_384


class BlobError(ValueError):
    """Invalid manifest, piece, or assembled object."""


def _address(tag: bytes, value: bytes) -> bytes:
    return sha256(tag + value).digest()


def object_address(value: bytes) -> bytes:
    return _address(OBJECT_TAG, bytes(value))


def piece_address(value: bytes) -> bytes:
    return _address(PIECE_TAG, bytes(value))


def manifest_address(manifest: dict[str, Any]) -> bytes:
    validate_manifest(manifest)
    return _address(MANIFEST_TAG, dumps(manifest))


def validate_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    keys = {"v", "type", "object", "length", "piece_size", "pieces"}
    if not isinstance(manifest, dict) or set(manifest) != keys:
        raise BlobError("invalid portable blob manifest")
    length = manifest["length"]
    piece_size = manifest["piece_size"]
    pieces = manifest["pieces"]
    valid_ints = (isinstance(length, int) and not isinstance(length, bool)
                  and length >= 0 and isinstance(piece_size, int)
                  and not isinstance(piece_size, bool)
                  and 0 < piece_size <= MAX_STRING_BYTES)
    if (not is_profile_version(manifest["v"]) or manifest["type"] != "blob/manifest"
            or not isinstance(manifest["object"], bytes)
            or len(manifest["object"]) != 32 or not valid_ints
            or not isinstance(pieces, list) or len(pieces) > MAX_PIECES
            or not all(isinstance(piece, bytes) and len(piece) == 32
                       for piece in pieces)):
        raise BlobError("invalid portable blob manifest")
    expected = (length + piece_size - 1) // piece_size
    if len(pieces) != expected:
        raise BlobError("invalid portable blob manifest")
    return manifest


def split_blob(value: bytes, piece_size: int = DEFAULT_PIECE_SIZE,
               max_pieces: int = MAX_PIECES) -> dict[str, Any]:
    if not isinstance(value, bytes):
        raise BlobError("blob value must be bytes")
    if (not isinstance(piece_size, int) or isinstance(piece_size, bool)
            or not 0 < piece_size <= MAX_STRING_BYTES
            or not isinstance(max_pieces, int) or isinstance(max_pieces, bool)
            or not 0 < max_pieces <= MAX_PIECES):
        raise BlobError("invalid blob chunking options")
    data = bytes(value)
    chunks = [data[index:index + piece_size]
              for index in range(0, len(data), piece_size)]
    if len(chunks) > max_pieces:
        raise BlobError("blob exceeds the maximum piece count")
    pieces = [{"index": index, "id": piece_address(chunk), "data": chunk}
              for index, chunk in enumerate(chunks)]
    manifest = {
        "v": 1,
        "type": "blob/manifest",
        "object": object_address(data),
        "length": len(data),
        "piece_size": piece_size,
        "pieces": [piece["id"] for piece in pieces],
    }
    return {
        "object_id": manifest["object"],
        "manifest_id": manifest_address(manifest),
        "manifest": manifest,
        "pieces": pieces,
    }


def expected_piece_length(manifest: dict[str, Any], index: int) -> int:
    validate_manifest(manifest)
    if (not isinstance(index, int) or isinstance(index, bool)
            or not 0 <= index < len(manifest["pieces"])):
        raise BlobError("piece index is outside the manifest")
    if index == len(manifest["pieces"]) - 1:
        return manifest["length"] - index * manifest["piece_size"]
    return manifest["piece_size"]


def verify_piece(manifest: dict[str, Any], index: int, data: bytes) -> bool:
    return (isinstance(data, bytes)
            and len(data) == expected_piece_length(manifest, index)
            and manifest["pieces"][index] == piece_address(data))


def need_bitmap(manifest: dict[str, Any],
                held: Collection[bytes] | Callable[[bytes], bool]) -> bytes:
    validate_manifest(manifest)
    contains = held if callable(held) else held.__contains__
    result = bytearray((len(manifest["pieces"]) + 7) // 8)
    for index, address in enumerate(manifest["pieces"]):
        if not contains(address):
            result[index // 8] |= 1 << (7 - index % 8)
    return bytes(result)


def needed_indices(manifest: dict[str, Any], bitmap: bytes) -> list[int]:
    validate_manifest(manifest)
    expected = (len(manifest["pieces"]) + 7) // 8
    if not isinstance(bitmap, bytes) or len(bitmap) != expected:
        raise BlobError("need bitmap has the wrong size")
    unused = expected * 8 - len(manifest["pieces"])
    if unused and bitmap[-1] & ((1 << unused) - 1):
        raise BlobError("need bitmap sets bits beyond the manifest")
    return [index for index in range(len(manifest["pieces"]))
            if bitmap[index // 8] & (1 << (7 - index % 8))]


def assemble(manifest: dict[str, Any], pieces: dict[int, bytes]) -> bytes:
    validate_manifest(manifest)
    ordered: list[bytes] = []
    for index in range(len(manifest["pieces"])):
        data = pieces.get(index)
        if data is None or not verify_piece(manifest, index, data):
            raise BlobError(f"missing or invalid blob piece {index}")
        ordered.append(data)
    value = b"".join(ordered)
    if len(value) != manifest["length"] or object_address(value) != manifest["object"]:
        raise BlobError("assembled blob does not match its object address")
    return value

