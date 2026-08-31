"""Portable Netz Authority-ID derivation.

This module deliberately has no signing dependency. It pins the genesis
object and self-certifying identifier so applications can inspect identities
with the dependency-free core; Ed25519 and Noise live in the optional crypto
client layer.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Iterable

from netz_codec import dumps

AUTHORITY_DOMAIN = b"netz/peer-id/v1\x00"
IDENTITY_RECORD_DOMAIN = b"netz/identity-record/v1\x00"
KEY_BYTES = 32


class IdentityError(ValueError):
    """A genesis object cannot define a Netz authority."""


def genesis(rotation: Iterable[bytes], threshold: int,
            operational: Iterable[bytes], revoke: bytes) -> dict[str, object]:
    """Validate and construct the profile-1 genesis object."""
    rotation = list(rotation)
    operational = list(operational)
    if (not rotation or not operational
            or type(threshold) is not int
            or not 1 <= threshold <= len(rotation)
            or not isinstance(revoke, bytes) or len(revoke) != KEY_BYTES
            or any(not isinstance(key, bytes) or len(key) != KEY_BYTES
                   for key in rotation + operational)):
        raise IdentityError("invalid Netz genesis fields")
    return {
        "v": 1,
        "rotation": rotation,
        "threshold": threshold,
        "operational": operational,
        "revoke": revoke,
    }


def genesis_bytes(value: dict[str, object]) -> bytes:
    """Return the exact deterministic CBOR hashed into an Authority ID."""
    expected = {"v", "rotation", "threshold", "operational", "revoke"}
    if not isinstance(value, dict) or set(value) != expected or value.get("v") != 1:
        raise IdentityError("invalid Netz genesis object")
    checked = genesis(value["rotation"], value["threshold"],
                      value["operational"], value["revoke"])
    return dumps(checked)


def authority_id(value: dict[str, object]) -> bytes:
    """Return the full 32-byte authentication identifier."""
    return sha256(AUTHORITY_DOMAIN + genesis_bytes(value)).digest()


def routing_id(value: dict[str, object]) -> bytes:
    """Return the 16-byte routing projection; never use it for authority."""
    return authority_id(value)[:16]


def identity_record_bytes(value: dict[str, object], operational: bytes,
                          addresses: Iterable[str], sequence: int) -> bytes:
    """Return the exact bytes signed by a profile-1 identity record."""
    genesis_bytes(value)  # validate before embedding the object
    addresses = sorted(addresses)
    if (not isinstance(operational, bytes) or len(operational) != KEY_BYTES
            or type(sequence) is not int or sequence < 0
            or any(not isinstance(address, str) for address in addresses)):
        raise IdentityError("invalid identity record fields")
    return IDENTITY_RECORD_DOMAIN + dumps({
        "v": 1,
        "genesis": value,
        "operational": operational,
        "seq": sequence,
        "addresses": addresses,
    })


__all__ = [
    "AUTHORITY_DOMAIN",
    "IDENTITY_RECORD_DOMAIN",
    "IdentityError",
    "authority_id",
    "genesis",
    "genesis_bytes",
    "identity_record_bytes",
    "routing_id",
]
