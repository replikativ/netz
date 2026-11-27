"""Portable application-object bindings for explicit local materialization."""

from __future__ import annotations

from hashlib import sha256
from typing import Any

from netz_codec import dumps, is_profile_version

BINDING_TAG = b"netz/application-object/v1\x00"
MAX_STORE_ID_BYTES = 256
MAX_APPLICATION_KEY_BYTES = 4096
MAX_CODEC_ID_BYTES = 128


class MaterializationError(ValueError):
    """Invalid or mismatched authenticated application-object binding."""


def _bounded_id(value: Any, limit: int) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    return (all(0x21 <= ord(character) <= 0x7e for character in value)
            and len(value) <= limit)


def validate_binding(binding: dict[str, Any]) -> dict[str, Any]:
    """Validate the closed draft-00 application-object binding schema."""
    fields = {
        "v", "type", "store_id", "application_key", "codec", "object",
        "manifest",
    }
    valid = (
        isinstance(binding, dict)
        and set(binding) == fields
        and is_profile_version(binding["v"])
        and binding["type"] == "blob/application-object"
        and _bounded_id(binding["store_id"], MAX_STORE_ID_BYTES)
        and isinstance(binding["application_key"], bytes)
        and 0 < len(binding["application_key"]) <= MAX_APPLICATION_KEY_BYTES
        and _bounded_id(binding["codec"], MAX_CODEC_ID_BYTES)
        and isinstance(binding["object"], bytes)
        and len(binding["object"]) == 32
        and isinstance(binding["manifest"], bytes)
        and len(binding["manifest"]) == 32
    )
    if not valid:
        raise MaterializationError("invalid application-object binding")
    return binding


def application_binding(*, store_id: str, application_key: bytes, codec: str,
                        object_id: bytes, manifest_id: bytes) -> dict[str, Any]:
    """Create one portable binding for an authenticated catalog or root."""
    return validate_binding({
        "v": 1,
        "type": "blob/application-object",
        "store_id": store_id,
        "application_key": application_key,
        "codec": codec,
        "object": object_id,
        "manifest": manifest_id,
    })


def binding_address(binding: dict[str, Any]) -> bytes:
    """Return the domain-separated canonical-CBOR address of ``binding``."""
    return sha256(BINDING_TAG + dumps(validate_binding(binding))).digest()


def validate_retention_binding(binding: dict[str, Any],
                               retention: dict[str, Any]) -> dict[str, Any]:
    """Fail unless a validated binding names the retention object and manifest.

    The caller is responsible for signature verification and reconciliation of
    the retention statement and the catalog/root carrying the binding.
    """
    validate_binding(binding)
    if (not isinstance(retention, dict)
            or retention.get("type") != "blob/retention"
            or binding["object"] != retention.get("object")
            or binding["manifest"] != retention.get("manifest")):
        raise MaterializationError(
            "application binding does not match retention")
    return binding
