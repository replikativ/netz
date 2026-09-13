"""Independent verification of controller-authorized replica retirement."""

from __future__ import annotations

from typing import Any, Iterable

from .replication import place, satisfies_domains
from .replication_layout import layout_address, placement_layout
from .replication_wire import (
    MAX_SAFE_INTEGER,
    PERSISTENCE_PROFILES,
    parse_envelope,
    verify_envelope,
)


class RetirementError(ValueError):
    """A retirement command or its replacement evidence is ineligible."""


def _authority_hex(value: bytes | str) -> str:
    if isinstance(value, bytes) and len(value) == 32:
        return value.hex()
    if (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdefABCDEF" for character in value)):
        return value.lower()
    raise RetirementError("invalid Authority ID")


def verify_command(envelope: dict[str, Any], *, layout_state: dict[str, Any],
                   local_authority: bytes | str,
                   controllers: Iterable[bytes | str],
                   accepted_persistence: set[str], now: int) -> dict[str, Any]:
    """Verify all N replacement receipts and return bounded evidence.

    This authorizes consideration of a local delete. Storage protection and
    lifecycle checks still have to be serialized with physical deletion.
    """
    try:
        if (not verify_envelope(envelope) or not isinstance(layout_state, dict)
                or layout_state.get("conflicts")
                or not isinstance(accepted_persistence, (set, frozenset))
                or not accepted_persistence
                or not accepted_persistence <= PERSISTENCE_PROFILES
                or "netz/store/volatile/1" in accepted_persistence
                or type(now) is not int or not 0 <= now <= MAX_SAFE_INTEGER):
            raise RetirementError("invalid retirement verification context")
        command = parse_envelope(envelope)["statement"]
        current = layout_state["current"]["statement"]
        retention_envelope = command["retention"]
        if not verify_envelope(retention_envelope):
            raise RetirementError("invalid current retention signature")
        retention = parse_envelope(retention_envelope)["statement"]
        receipts: list[dict[str, Any]] = []
        for receipt_envelope in command["receipts"]:
            if not verify_envelope(receipt_envelope):
                raise RetirementError("invalid replacement receipt signature")
            receipts.append(parse_envelope(receipt_envelope)["statement"])
        policy = {
            "replicas": retention["replicas"],
            "write_acks": retention["write_acks"],
            "placement_minimum_domains":
                retention["placement_minimum_domains"],
            "ack_minimum_domains": retention["ack_minimum_domains"],
        }
        selected = place(placement_layout(current), policy, command["object"])
        targets = {member["id"] for member in selected}
        receipt_targets = {_authority_hex(receipt["replica"])
                           for receipt in receipts}
        controller_ids = {_authority_hex(value) for value in controllers}
        signer = _authority_hex(command["signer"])
        local = _authority_hex(local_authority)
        common = ("cluster", "privacy_domain", "manifest", "object")
        receipts_match = all(
            receipt["type"] == "blob/stored"
            and _authority_hex(receipt["replica"]) in targets
            and receipt["persistence"] in accepted_persistence
            and all(receipt[key] == command[key] for key in common)
            and receipt["layout_epoch"] == command["layout_epoch"]
            and receipt["retention_revision"] == retention["revision"]
            and receipt["retain_until"] == retention["retain_until"]
            and receipt["stored_at"] <= now < receipt["retain_until"]
            for receipt in receipts
        )
        valid = (
            command["type"] == "blob/retirement"
            and controller_ids == layout_state["controllers"]
            and signer in controller_ids
            and _authority_hex(current["signer"]) in controller_ids
            and signer == _authority_hex(retention["signer"])
            and local == _authority_hex(command["replica"])
            and local not in targets
            and layout_state["cluster"] == command["cluster"]
            and layout_state["current"]["address"] == layout_address(current)
            and layout_address(current) == command["layout"]
            and current["previous"] == command["previous_layout"]
            and current["epoch"] > 0
            and current["epoch"] == command["layout_epoch"]
            and retention["layout_epoch"] == command["layout_epoch"]
            and all(command[key] == retention[key] for key in common)
            and retention["revision"] < command["revision"]
            and retention["issued_at"] <= command["issued_at"] <= now
            and command["retire_after"] <= now < retention["retain_until"]
            and len(receipts) == retention["replicas"]
            and targets == receipt_targets
            and receipts_match
            and satisfies_domains(
                selected, retention["placement_minimum_domains"])
        )
        if not valid:
            raise RetirementError("retirement evidence is not currently sufficient")
        return {
            "statement": command,
            "retention": retention,
            "targets": targets,
            "receipts": {_authority_hex(receipt["replica"]): receipt
                         for receipt in receipts},
        }
    except RetirementError:
        raise
    except Exception as error:
        raise RetirementError("invalid retirement evidence") from error


__all__ = ["RetirementError", "verify_command"]
