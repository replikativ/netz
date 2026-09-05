"""Experimental blob-placement/2; not enabled by signed-layout parsing.

The explicit pool seed is stable across authority epochs. This prototype must
not be used to reinterpret existing profile-1 layouts or receipts.
"""

from hashlib import sha256
from itertools import combinations
import re

from netz_codec import dumps
from .replication import (
    MAX_CONSTRAINED_COMBINATIONS, ReplicationError, eligible_members,
    validate_policy,
)


def score(seed, object_id, member_id):
    if not isinstance(seed, str) or not re.fullmatch(r"[0-9a-f]{64}", seed):
        raise ReplicationError("placement seed must be 32 bytes as lowercase hex")
    return sha256(dumps({"seed": seed, "member": member_id,
                         "object": object_id,
                         "profile": "netz/blob-placement/2"})).digest()


def rank(layout, seed, object_id):
    # Validate even an empty eligible set.
    score(seed, object_id, "")
    return sorted(eligible_members(layout),
                  key=lambda member: (-int.from_bytes(
                      score(seed, object_id, member["id"]), "big"), member["id"]))


def place(layout, seed, policy, object_id):
    policy = validate_policy(layout, policy)
    for examined, candidate in enumerate(combinations(
            rank(layout, seed, object_id), policy["replicas"])):
        if examined >= MAX_CONSTRAINED_COMBINATIONS:
            raise ReplicationError("constrained placement search exhausted")
        if all(len({member.get("domains", {})[domain]
                    for member in candidate
                    if domain in member.get("domains", {})}) >= minimum
               for domain, minimum in policy["minimum_domains"].items()):
            return list(candidate)
    raise ReplicationError("no replica set satisfies all failure domains")
