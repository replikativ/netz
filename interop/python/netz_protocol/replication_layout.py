"""Signed, hash-chained replica layouts for durable blob profile 1."""

from __future__ import annotations

from hashlib import sha256
import re
from typing import Any

from netz_codec import dumps

from .replication import MAX_LAYOUT_MEMBERS, place

STATEMENT_TYPE = "blob/layout"
ADDRESS_DOMAIN = b"netz/blob-layout/v1\x00"
MAX_SAFE_INTEGER = (1 << 53) - 1
MAX_DOMAINS_PER_MEMBER = 16
DEFAULT_MAX_CONFLICTS = 32
ZERO_ADDRESS = b"\x00" * 32
AUTHORITY_ID = re.compile(r"^[0-9a-fA-F]{64}$")
STATEMENT_FIELDS = {
    "v", "type", "cluster", "epoch", "previous", "members", "issued_at",
    "signer", "placement", "seed",
}
MEMBER_FIELDS = {"authority", "domains", "enabled"}


class LayoutError(ValueError):
    """A signed layout or reconciliation configuration is invalid."""


def _uint(value: Any) -> bool:
    return type(value) is int and 0 <= value <= MAX_SAFE_INTEGER


def _bytes_n(value: Any, size: int) -> bool:
    return isinstance(value, bytes) and len(value) == size


def _cluster_id(value: Any) -> bool:
    return isinstance(value, bytes) and 1 <= len(value) <= 64


def _visible_ascii(value: Any, limit: int = 128) -> bool:
    return (type(value) is str and 1 <= len(value) <= limit
            and all(0x21 <= ord(character) <= 0x7e for character in value))


def _authority_hex(value: bytes | str) -> str:
    if isinstance(value, bytes) and len(value) == 32:
        return value.hex()
    if isinstance(value, str) and AUTHORITY_ID.fullmatch(value):
        return value.lower()
    raise LayoutError("layout authority must be a full Authority ID")


def validate_statement(statement: dict[str, Any]) -> dict[str, Any]:
    """Validate one exact canonical signed-layout statement body."""
    if (not isinstance(statement, dict) or set(statement) != STATEMENT_FIELDS
            or statement.get("v") != 1
            or statement.get("type") != STATEMENT_TYPE
            or statement.get("placement") != "netz/blob-placement/2"
            or not _bytes_n(statement.get("seed"), 32)
            or not _cluster_id(statement.get("cluster"))
            or not _uint(statement.get("epoch"))
            or not _bytes_n(statement.get("previous"), 32)
            or not isinstance(statement.get("members"), list)
            or not 1 <= len(statement["members"]) <= MAX_LAYOUT_MEMBERS
            or not _uint(statement.get("issued_at"))
            or not _bytes_n(statement.get("signer"), 32)):
        raise LayoutError("invalid durable-blob layout statement")
    authorities: list[bytes] = []
    for member in statement["members"]:
        if (not isinstance(member, dict) or set(member) != MEMBER_FIELDS
                or not _bytes_n(member.get("authority"), 32)
                or not isinstance(member.get("domains"), dict)
                or len(member["domains"]) > MAX_DOMAINS_PER_MEMBER
                or not all(_visible_ascii(key) for key in member["domains"])
                or not all(_visible_ascii(value)
                           for value in member["domains"].values())
                or type(member.get("enabled")) is not bool):
            raise LayoutError("invalid durable-blob layout member")
        authorities.append(member["authority"])
    if authorities != sorted(authorities) or len(authorities) != len(
            set(authorities)):
        raise LayoutError("layout members must be unique and sorted")
    if statement["epoch"] == 0 and statement["previous"] != ZERO_ADDRESS:
        raise LayoutError("genesis layout must use zero previous address")
    return statement


def layout_address(statement: dict[str, Any]) -> bytes:
    """Return the domain-separated address of a canonical layout."""
    return sha256(ADDRESS_DOMAIN + dumps(validate_statement(statement))).digest()


def placement_layout(statement: dict[str, Any]) -> dict[str, Any]:
    """Convert a verified signed layout to the placement engine shape."""
    validate_statement(statement)
    return {
        "epoch": statement["epoch"],
        "placement": statement["placement"],
        "seed": statement["seed"].hex(),
        "members": [
            {
                "id": member["authority"].hex(),
                "domains": member["domains"],
                "enabled": member["enabled"],
            }
            for member in statement["members"]
        ],
    }


def make_state(cluster: bytes, controllers: list[bytes | str],
               max_conflicts: int = DEFAULT_MAX_CONFLICTS) -> dict[str, Any]:
    """Create a pure layout-chain reconciler state."""
    controller_ids = {_authority_hex(controller) for controller in controllers}
    if (not _cluster_id(cluster) or not controller_ids
            or type(max_conflicts) is not int or max_conflicts <= 0):
        raise LayoutError("invalid layout reconciliation options")
    return {
        "cluster": cluster,
        "controllers": controller_ids,
        "current": None,
        "conflicts": [],
        "max_conflicts": max_conflicts,
    }


def reconcile(state: dict[str, Any],
              statement: dict[str, Any]) -> dict[str, Any]:
    """Reconcile one already signature-verified statement into the chain."""
    try:
        validate_statement(statement)
        signer = statement["signer"].hex()
        address = layout_address(statement)
        current = state["current"]
        if (signer not in state["controllers"]
                or statement["cluster"] != state["cluster"]):
            return {"state": state, "status": "rejected"}
        if current is None:
            if statement["epoch"] != 0:
                return {"state": state, "status": "gap"}
            return {
                "state": {**state, "current": {
                    "statement": statement, "address": address}},
                "status": "accepted",
            }
        current_epoch = current["statement"]["epoch"]
        if statement["epoch"] < current_epoch:
            return {"state": state, "status": "stale"}
        if statement["epoch"] == current_epoch:
            if address == current["address"]:
                return {"state": state, "status": "duplicate"}
            proof = {
                "epoch": statement["epoch"],
                "accepted": current["address"].hex(),
                "conflicting": address.hex(),
                "statement": statement,
            }
            conflicts = state["conflicts"]
            if not any(item["conflicting"] == address.hex()
                       for item in conflicts):
                conflicts = (conflicts + [proof])[-state["max_conflicts"]:]
            return {"state": {**state, "conflicts": conflicts},
                    "status": "conflict"}
        if state["conflicts"]:
            return {"state": state, "status": "forked"}
        if statement["epoch"] > current_epoch + 1:
            return {"state": state, "status": "gap"}
        if statement["previous"] != current["address"]:
            return {"state": state, "status": "rejected"}
        if statement["seed"] != current["statement"]["seed"]:
            return {"state": state, "status": "rejected"}
        return {
            "state": {**state, "current": {
                "statement": statement, "address": address}},
            "status": "accepted",
        }
    except Exception:
        return {"state": state, "status": "rejected"}


def retention_authorized(state: dict[str, Any], local_authority: bytes | str,
                         retention: dict[str, Any]) -> bool:
    """Return whether the current layout selects this replica for retention."""
    try:
        statement = state["current"]["statement"]
        targets = place(
            placement_layout(statement),
            {
                "replicas": retention["replicas"],
                "write_acks": retention["write_acks"],
                "placement_minimum_domains":
                    retention["placement_minimum_domains"],
                "ack_minimum_domains": retention["ack_minimum_domains"],
            },
            retention["object"],
        )
        return (not state["conflicts"]
                and retention["type"] == "blob/retention"
                and retention["cluster"] == state["cluster"]
                and retention["layout_epoch"] == statement["epoch"]
                and _authority_hex(local_authority)
                in {target["id"] for target in targets})
    except Exception:
        return False
