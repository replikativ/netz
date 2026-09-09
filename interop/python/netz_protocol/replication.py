"""Deterministic placement for the Netz durable-blob extension draft.

Discovery does not participate in placement.  A trusted layout supplies the
eligible replica identities and their operator-declared failure domains;
rendezvous hashing chooses targets identically in every implementation.
"""

from __future__ import annotations

from functools import cmp_to_key
from hashlib import sha256
from itertools import combinations
import re
from typing import Any, Iterable

from netz_codec import dumps

MAX_LAYOUT_MEMBERS = 32
MAX_REPLICAS = 5
MAX_CONSTRAINED_COMBINATIONS = 250_000
AUTHORITY_ID = re.compile(r"^[0-9a-f]{64}$")


class ReplicationError(ValueError):
    """Invalid or unsatisfiable replica configuration."""


def _uint(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_layout(layout: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(layout, dict):
        raise ReplicationError("invalid replica layout")
    members = layout.get("members")
    if (not _uint(layout.get("epoch")) or not isinstance(members, list)
            or len(members) > MAX_LAYOUT_MEMBERS):
        raise ReplicationError("invalid replica layout")
    ids: list[str] = []
    for member in members:
        if not isinstance(member, dict):
            raise ReplicationError("invalid replica layout")
        member_id = member.get("id")
        domains = member.get("domains", {})
        enabled = member.get("enabled", True)
        if (not isinstance(member_id, str) or not AUTHORITY_ID.fullmatch(member_id)
                or not isinstance(domains, dict)
                or not all(isinstance(key, str) for key in domains)
                or not all(isinstance(value, str) and value
                           for value in domains.values())
                or not isinstance(enabled, bool)):
            raise ReplicationError("invalid replica layout")
        ids.append(member_id)
    if len(ids) != len(set(ids)):
        raise ReplicationError("invalid replica layout")
    return layout


def eligible_members(layout: dict[str, Any]) -> list[dict[str, Any]]:
    validate_layout(layout)
    return [member for member in layout["members"]
            if member.get("enabled", True)]


def validate_policy(layout: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    eligible = eligible_members(layout)
    if not isinstance(policy, dict):
        raise ReplicationError("invalid replica policy")
    normalized = {**policy, "minimum_domains": policy.get("minimum_domains", {})}
    replicas = normalized.get("replicas")
    write_acks = normalized.get("write_acks")
    minima = normalized["minimum_domains"]
    if (not _uint(replicas) or replicas == 0 or replicas > MAX_REPLICAS
            or not _uint(write_acks) or write_acks == 0
            or write_acks > replicas or replicas > len(eligible)
            or not isinstance(minima, dict)
            or not all(isinstance(key, str) for key in minima)
            or not all(_uint(value) and 0 < value <= write_acks
                       for value in minima.values())):
        raise ReplicationError("invalid replica policy")
    for domain, minimum in minima.items():
        available = {member.get("domains", {}).get(domain)
                     for member in eligible
                     if member.get("domains", {}).get(domain) is not None}
        if len(available) < minimum:
            raise ReplicationError(
                f"replica layout cannot satisfy failure domain {domain!r}")
    return normalized


def score(seed: str, object_id: bytes | str, member_id: str) -> bytes:
    """Return the portable rendezvous score for one member and object."""
    if (not isinstance(seed, str) or len(seed) != 64
            or any(character not in "0123456789abcdef" for character in seed)):
        raise ReplicationError("placement seed must be 32 bytes as lowercase hex")
    return sha256(dumps({
        "seed": seed,
        "member": member_id,
        "object": object_id,
        "profile": "netz/blob-placement/2",
    })).digest()


def rank(layout: dict[str, Any], object_id: bytes | str) -> list[dict[str, Any]]:
    """Eligible members in descending score order, identity ascending on ties."""
    if layout.get("placement") != "netz/blob-placement/2":
        raise ReplicationError("unsupported placement profile")
    score(layout.get("seed"), object_id, "")
    members = eligible_members(layout)

    def compare(left: dict[str, Any], right: dict[str, Any]) -> int:
        left_score = score(layout["seed"], object_id, left["id"])
        right_score = score(layout["seed"], object_id, right["id"])
        if left_score != right_score:
            return -1 if left_score > right_score else 1
        if left["id"] < right["id"]:
            return -1
        if left["id"] > right["id"]:
            return 1
        return 0

    return sorted(members, key=cmp_to_key(compare))


def domain_counts(members: Iterable[dict[str, Any]],
                  minima: dict[str, int]) -> dict[str, int]:
    materialized = list(members)
    return {
        domain: len({member.get("domains", {}).get(domain)
                     for member in materialized
                     if member.get("domains", {}).get(domain) is not None})
        for domain in minima
    }


def satisfies_domains(members: Iterable[dict[str, Any]],
                      minima: dict[str, int]) -> bool:
    counts = domain_counts(members, minima)
    return all(counts.get(domain, 0) >= minimum
               for domain, minimum in minima.items())


def place(layout: dict[str, Any], policy: dict[str, Any],
          object_id: bytes | str) -> list[dict[str, Any]]:
    """Choose the first rendezvous-ranked set satisfying all failure domains."""
    normalized = validate_policy(layout, policy)
    ranked = rank(layout, object_id)
    replicas = normalized["replicas"]
    minima = normalized["minimum_domains"]
    preferred = ranked[:replicas]
    if satisfies_domains(preferred, minima):
        return preferred
    for examined, candidate in enumerate(combinations(ranked, replicas), start=1):
        if examined > MAX_CONSTRAINED_COMBINATIONS:
            raise ReplicationError("constrained placement search exhausted")
        if satisfies_domains(candidate, minima):
            return list(candidate)
    raise ReplicationError("no replica set satisfies all failure domains")
