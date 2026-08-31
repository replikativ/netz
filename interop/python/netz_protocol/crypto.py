"""Optional Ed25519 identity and Noise-static binding helpers.

Importing :mod:`netz_protocol` remains dependency-free.  Applications that
need to create or verify signed identities install ``netz-protocol[crypto]``
and import this module explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from netz_codec import dumps, loads

from .identity import (
    KEY_BYTES,
    IdentityError,
    authority_id,
    genesis_bytes,
    identity_record_bytes,
)

NOISE_PROTOCOL_NAME = b"Noise_XX_25519_AESGCM_SHA256"
NOISE_PROLOGUE = b"netz/noise/v1"
NOISE_STATIC_DOMAIN = b"netz/noise-static/v1\x00"
SIGNATURE_BYTES = 64
MAX_ADDRESSES = 16
MAX_CAPABILITIES = 64


class CryptoError(ValueError):
    """A signed identity or Noise binding is malformed or invalid."""


def _ed25519():
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
    except ImportError as error:  # pragma: no cover - depends on installation
        raise RuntimeError(
            "signed identities require 'netz-protocol[crypto]'"
        ) from error
    return ed25519


def _public_bytes(private_key: bytes) -> bytes:
    if not isinstance(private_key, bytes) or len(private_key) != KEY_BYTES:
        raise CryptoError("Ed25519 private key must be 32 bytes")
    ed25519 = _ed25519()
    return ed25519.Ed25519PrivateKey.from_private_bytes(private_key).public_key().public_bytes_raw()


def sign_identity_record(genesis: dict[str, object], private_key: bytes,
                         addresses: Iterable[str], sequence: int) -> dict[str, object]:
    """Construct the exact signed identity record carried in a Noise payload."""
    public_key = _public_bytes(private_key)
    try:
        operational = genesis["operational"]
    except (KeyError, TypeError) as error:
        raise CryptoError("invalid genesis object") from error
    if public_key not in operational:
        raise CryptoError("signing key is not authorized by genesis")
    addresses = list(addresses)
    if any(not isinstance(address, str) for address in addresses):
        raise CryptoError("identity addresses must be strings")
    addresses = sorted(set(addresses))
    if len(addresses) > MAX_ADDRESSES:
        raise CryptoError("identity record has more than 16 addresses")
    transcript = identity_record_bytes(genesis, public_key, addresses, sequence)
    signature = _ed25519().Ed25519PrivateKey.from_private_bytes(private_key).sign(transcript)
    return {
        "v": 1,
        "genesis": genesis,
        "operational": public_key,
        "seq": sequence,
        "addresses": addresses,
        "signature": signature,
    }


def verify_identity_record(record: Mapping[str, object]) -> bytes:
    """Verify one exact-schema record and return its full Authority ID."""
    expected = {"v", "genesis", "operational", "seq", "addresses", "signature"}
    if not isinstance(record, dict) or set(record) != expected or record.get("v") != 1:
        raise CryptoError("invalid identity record schema")
    genesis = record["genesis"]
    operational = record["operational"]
    addresses = record["addresses"]
    sequence = record["seq"]
    signature = record["signature"]
    if (not isinstance(genesis, dict)
            or not isinstance(operational, bytes) or len(operational) != KEY_BYTES
            or not isinstance(addresses, list)
            or len(addresses) > MAX_ADDRESSES
            or addresses != sorted(set(addresses))
            or any(not isinstance(address, str) for address in addresses)
            or type(sequence) is not int or sequence < 0
            or not isinstance(signature, bytes) or len(signature) != SIGNATURE_BYTES):
        raise CryptoError("invalid identity record fields")
    try:
        genesis_bytes(genesis)
    except IdentityError as error:
        raise CryptoError("invalid genesis object") from error
    if operational not in genesis["operational"]:
        raise CryptoError("record key is not authorized by genesis")
    transcript = identity_record_bytes(genesis, operational, addresses, sequence)
    try:
        _ed25519().Ed25519PublicKey.from_public_bytes(operational).verify(
            signature, transcript
        )
    except Exception as error:
        raise CryptoError("invalid identity record signature") from error
    return authority_id(genesis)


def noise_static_binding_bytes(authority: bytes, noise_static: bytes) -> bytes:
    """Return the deterministic operational-key signature transcript."""
    if (not isinstance(authority, bytes) or len(authority) != KEY_BYTES
            or not isinstance(noise_static, bytes) or len(noise_static) != KEY_BYTES):
        raise CryptoError("authority and Noise static key must be 32 bytes")
    return NOISE_STATIC_DOMAIN + dumps({
        "v": 1,
        "authority": authority,
        "protocol": NOISE_PROTOCOL_NAME.decode("ascii"),
        "static": noise_static,
    })


@dataclass(frozen=True)
class VerifiedPeer:
    """Authenticated peer data returned by the secure transport upgrade."""

    authority: bytes
    routing_id: bytes
    operational: bytes
    noise_static: bytes
    addresses: tuple[str, ...]
    sequence: int
    capabilities: tuple[str, ...]


def make_identity_payload(record: dict[str, object], private_key: bytes,
                          noise_static: bytes,
                          capabilities: Iterable[str]) -> bytes:
    """Encode the identity payload for XX messages 2 or 3."""
    authority = verify_identity_record(record)
    if _public_bytes(private_key) != record["operational"]:
        raise CryptoError("payload signing key differs from identity record")
    capabilities = list(capabilities)
    if any(not isinstance(capability, str) or not capability
           for capability in capabilities):
        raise CryptoError("capabilities must be non-empty strings")
    capabilities = sorted(set(capabilities))
    if len(capabilities) > MAX_CAPABILITIES:
        raise CryptoError("identity payload has more than 64 capabilities")
    signature = _ed25519().Ed25519PrivateKey.from_private_bytes(private_key).sign(
        noise_static_binding_bytes(authority, noise_static)
    )
    return dumps({
        "v": 1,
        "record": record,
        "noise_static": noise_static,
        "noise_signature": signature,
        "capabilities": capabilities,
    })


def verify_identity_payload(payload: bytes, actual_noise_static: bytes,
                            expected_authority: bytes | None = None) -> VerifiedPeer:
    """Verify payload, expected dial target, and the actual XX static key."""
    try:
        value = loads(payload)
    except Exception as error:
        raise CryptoError("invalid identity payload encoding") from error
    expected = {"v", "record", "noise_static", "noise_signature", "capabilities"}
    if not isinstance(value, dict) or set(value) != expected or value.get("v") != 1:
        raise CryptoError("invalid identity payload schema")
    record = value["record"]
    claimed_static = value["noise_static"]
    signature = value["noise_signature"]
    capabilities = value["capabilities"]
    if (not isinstance(record, dict)
            or not isinstance(claimed_static, bytes) or len(claimed_static) != KEY_BYTES
            or not isinstance(actual_noise_static, bytes) or len(actual_noise_static) != KEY_BYTES
            or claimed_static != actual_noise_static
            or not isinstance(signature, bytes) or len(signature) != SIGNATURE_BYTES
            or not isinstance(capabilities, list)
            or len(capabilities) > MAX_CAPABILITIES
            or capabilities != sorted(set(capabilities))
            or any(not isinstance(capability, str) or not capability
                   for capability in capabilities)):
        raise CryptoError("invalid identity payload fields")
    authority = verify_identity_record(record)
    if expected_authority is not None and authority != expected_authority:
        raise CryptoError("remote authority differs from expected dial target")
    try:
        _ed25519().Ed25519PublicKey.from_public_bytes(record["operational"]).verify(
            signature, noise_static_binding_bytes(authority, claimed_static)
        )
    except Exception as error:
        raise CryptoError("invalid Noise static-key binding") from error
    return VerifiedPeer(
        authority=authority,
        routing_id=authority[:16],
        operational=record["operational"],
        noise_static=claimed_static,
        addresses=tuple(record["addresses"]),
        sequence=record["seq"],
        capabilities=tuple(capabilities),
    )


__all__ = [
    "CryptoError",
    "MAX_ADDRESSES",
    "MAX_CAPABILITIES",
    "NOISE_PROLOGUE",
    "NOISE_PROTOCOL_NAME",
    "NOISE_STATIC_DOMAIN",
    "VerifiedPeer",
    "make_identity_payload",
    "noise_static_binding_bytes",
    "sign_identity_record",
    "verify_identity_payload",
    "verify_identity_record",
]
