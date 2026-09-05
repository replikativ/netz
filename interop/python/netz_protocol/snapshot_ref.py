"""Portable signed references to immutable DAG snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping, cast

from netz_codec import dumps, loads

from .identity import KEY_BYTES, IdentityError, authority_id, genesis_bytes

SIGNING_DOMAIN = b"netz/snapshot-ref/v1\x00"
SIGNATURE_BYTES = 64
MAX_PROFILE_BYTES = 128
MAX_ID_BYTES = 256
MAX_PARENTS = 64
MAX_ORDERING_BYTES = 512


class SnapshotRefError(ValueError):
    """A snapshot-reference statement is malformed or unauthenticated."""


def _ed25519():
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError("signed references require 'netz-protocol[crypto]'") from error
    return ed25519


def _bounded_bytes(value: object, limit: int) -> bytes:
    if not isinstance(value, bytes) or not 0 < len(value) <= limit:
        raise SnapshotRefError("identifier is not a bounded byte string")
    return value


def _profile(value: object) -> str:
    if (type(value) is not str or not value
            or len(value.encode("utf-8")) > MAX_PROFILE_BYTES):
        raise SnapshotRefError("profile is not bounded UTF-8 text")
    return value


def signing_object(*, profile: str, system: bytes, ref: bytes,
                   snapshot: bytes, parents: Iterable[bytes], publisher: bytes,
                   ordering: bytes | None = None) -> dict[str, object]:
    """Return the exact deterministic-CBOR object covered by Ed25519."""
    profile = _profile(profile)
    system = _bounded_bytes(system, MAX_ID_BYTES)
    ref = _bounded_bytes(ref, MAX_ID_BYTES)
    snapshot = _bounded_bytes(snapshot, MAX_ID_BYTES)
    publisher = _bounded_bytes(publisher, KEY_BYTES)
    if len(publisher) != KEY_BYTES:
        raise SnapshotRefError("publisher must be a 32-byte Authority ID")
    canonical_parents = sorted(set(parents))
    if (len(canonical_parents) > MAX_PARENTS
            or snapshot in canonical_parents):
        raise SnapshotRefError("invalid parent set")
    for parent in canonical_parents:
        _bounded_bytes(parent, MAX_ID_BYTES)
    value: dict[str, object] = {
        "v": 1,
        "type": "snapshot-ref",
        "profile": profile,
        "system": system,
        "ref": ref,
        "snapshot": snapshot,
        "parents": canonical_parents,
        "publisher": publisher,
    }
    if ordering is not None:
        value["ordering"] = _bounded_bytes(ordering, MAX_ORDERING_BYTES)
    return value


def signing_bytes(**fields: object) -> bytes:
    return SIGNING_DOMAIN + dumps(signing_object(**fields))


def sign_snapshot_ref(genesis: dict[str, object], private_key: bytes,
                      **fields: object) -> dict[str, object]:
    """Create one self-verifying snapshot-reference envelope."""
    try:
        genesis_bytes(genesis)
        operational_keys = cast(Iterable[bytes], genesis["operational"])
    except (IdentityError, KeyError, TypeError) as error:
        raise SnapshotRefError("invalid genesis object") from error
    if not isinstance(private_key, bytes) or len(private_key) != KEY_BYTES:
        raise SnapshotRefError("Ed25519 private key must be 32 bytes")
    private = _ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
    operational = private.public_key().public_bytes_raw()
    if operational not in operational_keys:
        raise SnapshotRefError("signing key is not authorized by genesis")
    immutable = signing_object(
        **fields, publisher=authority_id(genesis)
    )
    return {
        **immutable,
        "genesis": genesis,
        "operational": operational,
        "signature": private.sign(SIGNING_DOMAIN + dumps(immutable)),
    }


def verify_snapshot_ref(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the exact schema, Authority binding, and signature."""
    required = {
        "v", "type", "profile", "system", "ref", "snapshot", "parents",
        "publisher", "genesis", "operational", "signature",
    }
    allowed = required | {"ordering"}
    if (not isinstance(value, dict) or not required.issubset(value)
            or not set(value).issubset(allowed)
            or value.get("v") != 1 or value.get("type") != "snapshot-ref"):
        raise SnapshotRefError("invalid snapshot-reference schema")
    genesis = value["genesis"]
    operational = value["operational"]
    signature = value["signature"]
    if (not isinstance(genesis, dict)
            or not isinstance(operational, bytes) or len(operational) != KEY_BYTES
            or not isinstance(signature, bytes) or len(signature) != SIGNATURE_BYTES):
        raise SnapshotRefError("invalid snapshot-reference credentials")
    try:
        genesis_bytes(genesis)
    except IdentityError as error:
        raise SnapshotRefError("invalid genesis object") from error
    immutable = signing_object(
        profile=cast(str, value["profile"]),
        system=cast(bytes, value["system"]),
        ref=cast(bytes, value["ref"]),
        snapshot=cast(bytes, value["snapshot"]),
        parents=cast(Iterable[bytes], value["parents"]),
        publisher=cast(bytes, value["publisher"]),
        ordering=cast(bytes | None, value.get("ordering")),
    )
    if (immutable["parents"] != value["parents"]
            or value["publisher"] != authority_id(genesis)
            or operational not in cast(Iterable[bytes], genesis["operational"])):
        raise SnapshotRefError("snapshot reference is not canonically authorized")
    try:
        _ed25519().Ed25519PublicKey.from_public_bytes(operational).verify(
            signature, SIGNING_DOMAIN + dumps(immutable)
        )
    except Exception as error:
        raise SnapshotRefError("invalid snapshot-reference signature") from error
    return dict(value)


def encode_snapshot_ref(value: Mapping[str, object]) -> bytes:
    return dumps(verify_snapshot_ref(value))


def decode_snapshot_ref(encoded: bytes) -> dict[str, object]:
    try:
        value = loads(encoded)
    except Exception as error:
        raise SnapshotRefError("invalid snapshot-reference encoding") from error
    return verify_snapshot_ref(value)


@dataclass
class SnapshotRefState:
    """Bounded in-memory DAG tracker for already verified envelopes."""

    trust_on_first_use: bool = True
    max_streams: int = 1024
    max_nodes_per_stream: int = 100_000
    owners: dict[tuple[str, bytes, bytes], bytes] = field(default_factory=dict)
    nodes: dict[tuple[str, bytes, bytes], dict[bytes, dict[str, object]]] = field(
        default_factory=dict
    )
    heads: dict[tuple[str, bytes, bytes], set[bytes]] = field(default_factory=dict)

    def pin(self, profile: str, system: bytes, ref: bytes, publisher: bytes) -> None:
        self.owners[(profile, system, ref)] = publisher

    @staticmethod
    def _ancestor(nodes: dict[bytes, dict[str, object]],
                  ancestor: bytes, descendant: bytes) -> bool:
        todo = list(cast(Iterable[bytes], nodes.get(descendant, {}).get("parents", [])))
        seen: set[bytes] = set()
        while todo:
            candidate = todo.pop()
            if candidate == ancestor:
                return True
            if candidate not in seen:
                seen.add(candidate)
                todo.extend(cast(Iterable[bytes],
                                 nodes.get(candidate, {}).get("parents", [])))
        return False

    def accept(self, envelope: Mapping[str, object]) -> str:
        statement = verify_snapshot_ref(envelope)
        stream = (cast(str, statement["profile"]),
                  cast(bytes, statement["system"]), cast(bytes, statement["ref"]))
        publisher = cast(bytes, statement["publisher"])
        owner = self.owners.get(stream)
        if owner is not None and owner != publisher:
            return "wrong-publisher"
        if owner is None and not self.trust_on_first_use:
            return "wrong-publisher"
        snapshot = cast(bytes, statement["snapshot"])
        parents = tuple(cast(Iterable[bytes], statement["parents"]))
        stream_nodes = self.nodes.get(stream, {})
        current = stream_nodes.get(snapshot)
        semantic = {"parents": parents, "ordering": statement.get("ordering")}
        if current is not None:
            return "duplicate" if current["semantic"] == semantic else "conflict"
        if any(self._ancestor(stream_nodes, snapshot, parent) for parent in parents):
            return "cycle"
        if ((stream not in self.nodes and len(self.nodes) >= self.max_streams)
                or len(stream_nodes) >= self.max_nodes_per_stream):
            return "capacity"
        self.owners[stream] = publisher
        stream_nodes = self.nodes.setdefault(stream, {})
        stream_nodes[snapshot] = {
            "parents": parents, "semantic": semantic, "statement": statement,
        }
        stream_heads = self.heads.setdefault(stream, set())
        if not any(self._ancestor(stream_nodes, snapshot, head)
                   for head in stream_heads):
            stream_heads.difference_update(
                head for head in tuple(stream_heads)
                if self._ancestor(stream_nodes, head, snapshot)
            )
            stream_heads.add(snapshot)
        return "accepted"


__all__ = [
    "SIGNING_DOMAIN", "SnapshotRefError", "SnapshotRefState",
    "decode_snapshot_ref", "encode_snapshot_ref", "sign_snapshot_ref",
    "signing_bytes", "signing_object", "verify_snapshot_ref",
]
