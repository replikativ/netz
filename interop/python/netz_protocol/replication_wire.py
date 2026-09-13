"""Signed statements and bounded frames for durable blob profile 1."""

from __future__ import annotations

from typing import Any

from netz_codec import MAX_STRING_BYTES, dumps, loads

from .blob import MAX_PIECES, manifest_address, validate_manifest
from .identity import KEY_BYTES, authority_id, genesis_bytes
from .replication_layout import (
    LayoutError,
    STATEMENT_TYPE as LAYOUT_STATEMENT_TYPE,
    validate_statement as validate_layout_statement,
)

CAPABILITY = "netz/blob-replication/1"
SIGNING_DOMAIN = b"netz/blob-statement/v1\x00"
SIGNATURE_BYTES = 64
MAX_SAFE_INTEGER = (1 << 53) - 1
PERSISTENCE_PROFILES = {
    "netz/store/volatile/1",
    "netz/store/best-effort/1",
    "netz/store/fsync/1",
}

STATEMENT_FIELDS = {
    "blob/retention": {
        "v", "type", "cluster", "privacy_domain", "revision", "manifest",
        "object", "layout_epoch", "replicas", "write_acks",
        "placement_minimum_domains", "ack_minimum_domains",
        "issued_at", "retain_until", "signer",
    },
    "blob/lease": {
        "v", "type", "cluster", "privacy_domain", "revision", "manifest",
        "object", "layout_epoch", "issued_at", "retain_until", "signer",
    },
    "blob/deletion": {
        "v", "type", "cluster", "privacy_domain", "revision", "manifest",
        "object", "layout_epoch", "issued_at", "delete_after", "signer",
    },
    "blob/retirement": {
        "v", "type", "cluster", "privacy_domain", "revision", "manifest",
        "object", "layout_epoch", "layout", "previous_layout", "replica",
        "retention", "receipts", "issued_at", "retire_after", "signer",
    },
    "blob/stored": {
        "v", "type", "cluster", "privacy_domain", "retention_revision",
        "manifest", "object", "layout_epoch", "replica", "attempt",
        "persistence", "stored_at", "retain_until", "signer",
    },
    "blob/deleted": {
        "v", "type", "cluster", "privacy_domain", "deletion_revision",
        "manifest", "object", "layout_epoch", "replica", "persistence",
        "deleted_at", "signer",
    },
}

FRAME_FIELDS = {
    "blob/offer": {"v", "type", "session", "attempt", "replica",
                   "retention", "manifest"},
    "blob/fetch": {"v", "type", "session", "attempt", "retention"},
    "blob/need": {"v", "type", "session", "manifest", "need"},
    "blob/piece": {"v", "type", "session", "manifest", "index", "data"},
    "blob/ack": {"v", "type", "session", "manifest", "through"},
    "blob/commit": {"v", "type", "session", "manifest", "count"},
    "blob/stored": {"v", "type", "session", "receipt"},
    "blob/abort": {"v", "type", "session", "code"},
}


class ReplicationWireError(ValueError):
    """A durable statement, credential envelope, or frame is invalid."""


def _uint(value: Any) -> bool:
    return type(value) is int and 0 <= value <= MAX_SAFE_INTEGER


def _positive_uint(value: Any) -> bool:
    return _uint(value) and value > 0


def _bytes_n(value: Any, size: int) -> bool:
    return isinstance(value, bytes) and len(value) == size


def _namespace_id(value: Any) -> bool:
    return isinstance(value, bytes) and 1 <= len(value) <= 64


def _text(value: Any, limit: int = 128) -> bool:
    return (type(value) is str and bool(value)
            and len(value.encode("utf-8")) <= limit)


def validate_statement(statement: dict[str, Any]) -> dict[str, Any]:
    """Validate one exact signed-statement body and return it."""
    if not isinstance(statement, dict):
        raise ReplicationWireError("invalid durable-blob statement")
    kind = statement.get("type")
    if kind == LAYOUT_STATEMENT_TYPE:
        try:
            return validate_layout_statement(statement)
        except LayoutError as error:
            raise ReplicationWireError("invalid durable-blob statement") from error
    fields = STATEMENT_FIELDS.get(kind)
    if (fields is None or set(statement) != fields or statement.get("v") != 1
            or not _namespace_id(statement.get("cluster"))
            or not _namespace_id(statement.get("privacy_domain"))
            or not _bytes_n(statement.get("manifest"), 32)
            or not _bytes_n(statement.get("object"), 32)
            or not _uint(statement.get("layout_epoch"))
            or not _bytes_n(statement.get("signer"), 32)):
        raise ReplicationWireError("invalid durable-blob statement")

    if kind == "blob/retention":
        replicas = statement["replicas"]
        write_acks = statement["write_acks"]
        placement_minima = statement["placement_minimum_domains"]
        ack_minima = statement["ack_minimum_domains"]
        valid = (
            _positive_uint(statement["revision"])
            and _positive_uint(replicas) and replicas <= 5
            and _positive_uint(write_acks) and write_acks <= replicas
            and isinstance(placement_minima, dict)
            and all(_text(key) for key in placement_minima)
            and all(_positive_uint(value) and value <= replicas
                    for value in placement_minima.values())
            and isinstance(ack_minima, dict)
            and all(_text(key) for key in ack_minima)
            and all(_positive_uint(value) and value <= write_acks
                    for value in ack_minima.values())
            and _uint(statement["issued_at"])
            and _uint(statement["retain_until"])
            and statement["issued_at"] <= statement["retain_until"]
        )
    elif kind == "blob/lease":
        valid = (_positive_uint(statement["revision"])
                 and _uint(statement["issued_at"])
                 and _uint(statement["retain_until"])
                 and statement["issued_at"] <= statement["retain_until"])
    elif kind == "blob/deletion":
        valid = (_positive_uint(statement["revision"])
                 and _uint(statement["issued_at"])
                 and _uint(statement["delete_after"])
                 and statement["issued_at"] <= statement["delete_after"])
    elif kind == "blob/retirement":
        try:
            retention = parse_envelope(statement["retention"])["statement"]
            receipts = statement["receipts"]
            receipt_statements = [parse_envelope(value)["statement"]
                                  for value in receipts]
            receipt_replicas = [value["replica"].hex()
                                for value in receipt_statements]
            nested_valid = (
                retention["type"] == "blob/retention"
                and type(receipts) is list and 1 <= len(receipts) <= 5
                and all(value["type"] == "blob/stored"
                        for value in receipt_statements)
                and receipt_replicas == sorted(receipt_replicas)
                and len(receipt_replicas) == len(set(receipt_replicas))
            )
        except Exception:
            nested_valid = False
        valid = (
            nested_valid
            and _positive_uint(statement["revision"])
            and _bytes_n(statement["layout"], 32)
            and _bytes_n(statement["previous_layout"], 32)
            and statement["layout"] != statement["previous_layout"]
            and _bytes_n(statement["replica"], 32)
            and _uint(statement["issued_at"])
            and _uint(statement["retire_after"])
            and statement["issued_at"] <= statement["retire_after"]
        )
    elif kind == "blob/stored":
        valid = (_positive_uint(statement["retention_revision"])
                 and _bytes_n(statement["replica"], 32)
                 and statement["replica"] == statement["signer"]
                 and _uint(statement["attempt"])
                 and statement["persistence"] in PERSISTENCE_PROFILES
                 and _uint(statement["stored_at"])
                 and _uint(statement["retain_until"])
                 and statement["stored_at"] <= statement["retain_until"])
    else:  # blob/deleted
        valid = (_positive_uint(statement["deletion_revision"])
                 and _bytes_n(statement["replica"], 32)
                 and statement["replica"] == statement["signer"]
                 and statement["persistence"] in PERSISTENCE_PROFILES
                 and _uint(statement["deleted_at"]))
    if not valid:
        raise ReplicationWireError("invalid durable-blob statement")
    return statement


def signing_bytes(statement: dict[str, Any]) -> bytes:
    """Return the exact domain-separated Ed25519 transcript."""
    return SIGNING_DOMAIN + dumps(validate_statement(statement))


def _validate_genesis(value: dict[str, Any]) -> dict[str, Any]:
    try:
        genesis_bytes(value)
    except Exception as error:
        raise ReplicationWireError("invalid statement genesis") from error
    return value


def make_envelope(genesis: dict[str, Any], operational: bytes,
                  statement: dict[str, Any], signature: bytes) -> dict[str, Any]:
    """Wrap a statement in self-contained Netz signing credentials."""
    validate_statement(statement)
    _validate_genesis(genesis)
    if (not _bytes_n(operational, KEY_BYTES)
            or operational not in genesis["operational"]
            or authority_id(genesis) != statement["signer"]
            or not _bytes_n(signature, SIGNATURE_BYTES)):
        raise ReplicationWireError("statement credentials do not authorize signer")
    return {
        "v": 1,
        "type": "blob/signed",
        "statement": statement,
        "genesis": genesis,
        "operational": operational,
        "signature": signature,
    }


def parse_envelope(value: dict[str, Any]) -> dict[str, Any]:
    expected = {"v", "type", "statement", "genesis", "operational", "signature"}
    if (not isinstance(value, dict) or set(value) != expected
            or value.get("v") != 1 or value.get("type") != "blob/signed"):
        raise ReplicationWireError("invalid durable-blob envelope")
    make_envelope(value["genesis"], value["operational"],
                  value["statement"], value["signature"])
    return value


def _ed25519():
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError("statement signing requires netz-protocol[crypto]") from error
    return ed25519


def sign_statement(genesis: dict[str, Any], private_key: bytes,
                   statement: dict[str, Any]) -> dict[str, Any]:
    """Sign and envelope a statement with an authorized raw Ed25519 key."""
    if not _bytes_n(private_key, KEY_BYTES):
        raise ReplicationWireError("Ed25519 private key must be 32 bytes")
    private = _ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
    operational = private.public_key().public_bytes_raw()
    return make_envelope(genesis, operational, statement,
                         private.sign(signing_bytes(statement)))


def verify_envelope(value: dict[str, Any]) -> bool:
    """Return false for malformed credentials or an invalid signature."""
    try:
        envelope = parse_envelope(value)
        (_ed25519().Ed25519PublicKey.from_public_bytes(envelope["operational"])
         .verify(envelope["signature"], signing_bytes(envelope["statement"])))
        return True
    except Exception:
        return False


def validate_frame(frame: dict[str, Any]) -> dict[str, Any]:
    """Validate one exact bounded transfer frame and return it."""
    if not isinstance(frame, dict):
        raise ReplicationWireError("invalid durable-blob frame")
    kind = frame.get("type")
    fields = FRAME_FIELDS.get(kind)
    if (fields is None or set(frame) != fields or frame.get("v") != 1
            or not _bytes_n(frame.get("session"), 16)):
        raise ReplicationWireError("invalid durable-blob frame")
    if kind == "blob/offer":
        try:
            retention = parse_envelope(frame["retention"])["statement"]
            manifest = validate_manifest(frame["manifest"])
            valid = (retention["type"] == "blob/retention"
                     and _uint(frame["attempt"])
                     and _bytes_n(frame["replica"], 32)
                     and retention["manifest"] == manifest_address(manifest)
                     and retention["object"] == manifest["object"])
        except Exception:
            valid = False
    elif kind == "blob/fetch":
        try:
            retention = parse_envelope(frame["retention"])["statement"]
            valid = (retention["type"] == "blob/retention"
                     and _uint(frame["attempt"]))
        except Exception:
            valid = False
    elif kind == "blob/need":
        valid = (_bytes_n(frame["manifest"], 32)
                 and isinstance(frame["need"], bytes)
                 and len(frame["need"]) <= (MAX_PIECES + 7) // 8)
    elif kind == "blob/piece":
        valid = (_bytes_n(frame["manifest"], 32) and _uint(frame["index"])
                 and isinstance(frame["data"], bytes)
                 and len(frame["data"]) <= MAX_STRING_BYTES)
    elif kind == "blob/ack":
        valid = _bytes_n(frame["manifest"], 32) and _uint(frame["through"])
    elif kind == "blob/commit":
        valid = _bytes_n(frame["manifest"], 32) and _uint(frame["count"])
    elif kind == "blob/stored":
        try:
            valid = parse_envelope(frame["receipt"])["statement"]["type"] == "blob/stored"
        except Exception:
            valid = False
    else:  # blob/abort
        valid = _text(frame["code"])
    if not valid:
        raise ReplicationWireError("invalid durable-blob frame")
    return frame


def encode_frame(frame: dict[str, Any]) -> bytes:
    return dumps(validate_frame(frame))


def decode_frame(data: bytes) -> dict[str, Any]:
    value = loads(data)
    if not isinstance(value, dict):
        raise ReplicationWireError("invalid durable-blob frame")
    return validate_frame(value)


__all__ = [
    "CAPABILITY", "PERSISTENCE_PROFILES", "ReplicationWireError",
    "decode_frame", "encode_frame", "make_envelope", "parse_envelope",
    "sign_statement", "signing_bytes", "validate_frame",
    "validate_statement", "verify_envelope",
]
