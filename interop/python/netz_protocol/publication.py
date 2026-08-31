"""Portable signed Netz publications with opaque application payloads."""

from __future__ import annotations

from typing import Iterable, Mapping

from netz_codec import dumps, loads

from .identity import KEY_BYTES, IdentityError, authority_id, genesis_bytes

GOSSIP_DOMAIN = b"netz/gossip/v1\x00"
SIGNATURE_BYTES = 64
MAX_SAFE_INTEGER = (1 << 53) - 1
MAX_TOPIC_BYTES = 1024
MAX_PAYLOAD_BYTES = 1024 * 1024
MAX_HOPS = MAX_SAFE_INTEGER


class PublicationError(ValueError):
    """A publication is malformed or unauthenticated."""


def _ed25519():
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError(
            "signed publications require 'netz-protocol[crypto]'"
        ) from error
    return ed25519


def _public_bytes(private_key: bytes) -> bytes:
    if not isinstance(private_key, bytes) or len(private_key) != KEY_BYTES:
        raise PublicationError("Ed25519 private key must be 32 bytes")
    return (_ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
            .public_key().public_bytes_raw())


def _checked_uint(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SAFE_INTEGER:
        raise PublicationError(f"{name} is outside the portable uint range")
    return value


def _checked_topic(value: object) -> str:
    if (type(value) is not str or not value
            or len(value.encode("utf-8")) > MAX_TOPIC_BYTES):
        raise PublicationError("topic must be bounded non-empty UTF-8 text")
    return value


def _checked_payload(value: object) -> bytes:
    if not isinstance(value, bytes) or len(value) > MAX_PAYLOAD_BYTES:
        raise PublicationError("payload must be bounded opaque bytes")
    return value


def signing_object(origin: bytes, epoch: int, sequence: int,
                   topic: str, payload: bytes) -> dict[str, object]:
    """Return the exact deterministic-CBOR object covered by Ed25519."""
    if not isinstance(origin, bytes) or len(origin) != KEY_BYTES:
        raise PublicationError("origin must be a full 32-byte Authority ID")
    return {
        "v": 1,
        "type": "gossip",
        "origin": origin,
        "epoch": _checked_uint(epoch, "epoch"),
        "seq": _checked_uint(sequence, "sequence"),
        "topic": _checked_topic(topic),
        "payload": _checked_payload(payload),
    }


def signing_bytes(origin: bytes, epoch: int, sequence: int,
                  topic: str, payload: bytes) -> bytes:
    """Return language-neutral bytes signed by a publication origin."""
    return GOSSIP_DOMAIN + dumps(
        signing_object(origin, epoch, sequence, topic, payload)
    )


def sign_publication(genesis: dict[str, object], private_key: bytes, *,
                     epoch: int, sequence: int, topic: str, payload: bytes,
                     hops: int = 0, repair: bool = False) -> dict[str, object]:
    """Construct one self-verifying profile-1 publication envelope."""
    try:
        genesis_bytes(genesis)
        operational_keys = genesis["operational"]
    except (IdentityError, KeyError, TypeError) as error:
        raise PublicationError("invalid genesis object") from error
    operational = _public_bytes(private_key)
    if operational not in operational_keys:
        raise PublicationError("signing key is not authorized by genesis")
    origin = authority_id(genesis)
    immutable = signing_object(origin, epoch, sequence, topic, payload)
    signature = (_ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
                 .sign(GOSSIP_DOMAIN + dumps(immutable)))
    envelope = {
        **immutable,
        "genesis": genesis,
        "operational": operational,
        "signature": signature,
        "hops": _checked_uint(hops, "hops"),
    }
    if repair:
        envelope["repair"] = True
    return envelope


def verify_publication(value: Mapping[str, object]) -> dict[str, object]:
    """Verify an exact-schema publication and return a plain dictionary."""
    required = {
        "v", "type", "origin", "epoch", "seq", "topic", "payload",
        "genesis", "operational", "signature", "hops",
    }
    allowed = required | {"repair"}
    if (not isinstance(value, dict) or not required.issubset(value)
            or not set(value).issubset(allowed)
            or value.get("v") != 1 or value.get("type") != "gossip"
            or ("repair" in value and value["repair"] is not True)):
        raise PublicationError("invalid publication schema")
    genesis = value["genesis"]
    operational = value["operational"]
    signature = value["signature"]
    if (not isinstance(genesis, dict)
            or not isinstance(operational, bytes) or len(operational) != KEY_BYTES
            or not isinstance(signature, bytes) or len(signature) != SIGNATURE_BYTES):
        raise PublicationError("invalid publication credentials")
    try:
        genesis_bytes(genesis)
    except IdentityError as error:
        raise PublicationError("invalid genesis object") from error
    origin = authority_id(genesis)
    if value["origin"] != origin or operational not in genesis["operational"]:
        raise PublicationError("publication origin is not authorized by genesis")
    immutable = signing_object(
        origin, value["epoch"], value["seq"], value["topic"], value["payload"]
    )
    _checked_uint(value["hops"], "hops")
    try:
        _ed25519().Ed25519PublicKey.from_public_bytes(operational).verify(
            signature, GOSSIP_DOMAIN + dumps(immutable)
        )
    except Exception as error:
        raise PublicationError("invalid publication signature") from error
    return dict(value)


def encode_publication(value: Mapping[str, object]) -> bytes:
    """Verify and deterministically encode one signed publication."""
    return dumps(verify_publication(value))


def decode_publication(encoded: bytes) -> dict[str, object]:
    """Decode and verify one deterministic publication envelope."""
    try:
        value = loads(encoded)
    except Exception as error:
        raise PublicationError("invalid publication encoding") from error
    return verify_publication(value)


__all__ = [
    "GOSSIP_DOMAIN", "PublicationError", "decode_publication",
    "encode_publication", "sign_publication", "signing_bytes",
    "signing_object", "verify_publication",
]
