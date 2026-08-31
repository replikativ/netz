"""Optional authenticated-Noise identity and capability helpers.

Importing :mod:`netz_protocol` remains dependency-free. Applications that
create or verify signed identities install ``netz-protocol[crypto]`` and import
this module explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
from typing import Iterable, Mapping
from urllib.parse import urlsplit, urlunsplit

from netz_codec import dumps, loads

from .identity import (
    KEY_BYTES,
    IdentityError,
    authority_id,
    genesis_bytes,
    identity_record_bytes,
)

NOISE_PROTOCOL_NAME = b"Noise_XX_25519_AESGCM_SHA256"
NOISE_PROLOGUE = b"netz/kabel-noise/v1\x00"
NOISE_STATIC_DOMAIN = b"netz/noise-static-binding/v1\x00"
DEFAULT_CAPABILITIES = ("netz/1", "transport/kabel-noise-cbor/1")
DEFAULT_REQUIRED = DEFAULT_CAPABILITIES
SIGNATURE_BYTES = 64
MAX_ADDRESSES = 16
MAX_ADDRESS_CHARS = 2048
MAX_CAPABILITIES = 64
MAX_CAPABILITY_CHARS = 128
MAX_SAFE_INTEGER = (1 << 53) - 1
ROLES = {"initiator", "responder"}
_VISIBLE_ASCII_MIN = 0x21
_VISIBLE_ASCII_MAX = 0x7E
_HEX = frozenset("0123456789abcdefABCDEF")
_PATH_SAFE = frozenset("/-._~!$&'()*+,;=:@")
_QUERY_SAFE = frozenset("/?-._~!$&'()*+,;=:@")


class CryptoError(ValueError):
    """A signed identity, offer, or Noise binding is invalid."""


def _ed25519():
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError(
            "signed identities require 'netz-protocol[crypto]'"
        ) from error
    return ed25519


def _public_bytes(private_key: bytes) -> bytes:
    if not isinstance(private_key, bytes) or len(private_key) != KEY_BYTES:
        raise CryptoError("Ed25519 private key must be 32 bytes")
    return (_ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
            .public_key().public_bytes_raw())


def _visible_ascii(value: str) -> bool:
    return all(_VISIBLE_ASCII_MIN <= ord(char) <= _VISIBLE_ASCII_MAX
               for char in value)


def _uri_component(value: str, safe: frozenset[str]) -> str:
    """Percent-encode UTF-8 while preserving and canonicalizing escapes."""
    result: list[str] = []
    offset = 0
    while offset < len(value):
        char = value[offset]
        if char == "%":
            if (offset + 2 >= len(value)
                    or value[offset + 1] not in _HEX
                    or value[offset + 2] not in _HEX):
                raise CryptoError("invalid percent escape in WebSocket URI")
            result.append("%" + value[offset + 1:offset + 3].upper())
            offset += 3
        elif (ord(char) < 128
              and (char.isalnum() or char in safe)):
            result.append(char)
            offset += 1
        else:
            result.extend(f"%{octet:02X}" for octet in char.encode("utf-8"))
            offset += 1
    return "".join(result)


def normalize_websocket_uri(value: str) -> str:
    """Return the visible-ASCII URI form used in signed identity records."""
    if type(value) is not str or not value or len(value) > MAX_ADDRESS_CHARS:
        raise CryptoError("invalid WebSocket URI")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError, UnicodeError) as error:
        raise CryptoError("invalid WebSocket URI") from error
    scheme = parsed.scheme.lower()
    if (scheme not in {"ws", "wss"} or not parsed.netloc
            or parsed.hostname is None or parsed.username is not None
            or parsed.password is not None or parsed.fragment):
        raise CryptoError("identity address must be an absolute ws/wss URI")

    host = parsed.hostname
    if "%" in host:
        raise CryptoError("scoped IPv6 is not allowed in an identity address")
    try:
        numeric_host = ipaddress.ip_address(host)
    except ValueError:
        try:
            ascii_host = host.encode("idna").decode("ascii").lower()
        except (UnicodeError, ValueError) as error:
            raise CryptoError("invalid IDNA host in WebSocket URI") from error
        if not ascii_host or not _visible_ascii(ascii_host):
            raise CryptoError("invalid IDNA host in WebSocket URI")
        netloc = ascii_host
    else:
        if numeric_host.version == 6:
            netloc = f"[{numeric_host.compressed.lower()}]"
        else:
            netloc = str(numeric_host)
    if port is not None:
        netloc += f":{port}"

    path = _uri_component(parsed.path or "/", _PATH_SAFE)
    query = _uri_component(parsed.query, _QUERY_SAFE)
    normalized = urlunsplit((scheme, netloc, path, query, ""))
    if (len(normalized) > MAX_ADDRESS_CHARS
            or not _visible_ascii(normalized)):
        raise CryptoError("normalized WebSocket URI is too long or non-ASCII")
    return normalized


def _validate_signed_websocket_uri(value: str) -> None:
    """Validate the interoperable signed form without rewriting its spelling."""
    if (type(value) is not str or not value or len(value) > MAX_ADDRESS_CHARS
            or not _visible_ascii(value)):
        raise CryptoError("invalid signed WebSocket URI")
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except (TypeError, ValueError, UnicodeError) as error:
        raise CryptoError("invalid signed WebSocket URI") from error
    if (parsed.scheme.lower() not in {"ws", "wss"}
            or not parsed.netloc or parsed.hostname is None
            or parsed.username is not None or parsed.password is not None
            or parsed.fragment):
        raise CryptoError("invalid signed WebSocket URI")


def canonical_capabilities(values: Iterable[str]) -> tuple[str, ...]:
    """Validate, sort, and return one canonical capability array."""
    values = list(values)
    if (len(values) > MAX_CAPABILITIES
            or any(type(value) is not str
                   or not 1 <= len(value) <= MAX_CAPABILITY_CHARS
                   or not _visible_ascii(value)
                   for value in values)):
        raise CryptoError("invalid capability set")
    if len(values) != len(set(values)):
        raise CryptoError("invalid capability set")
    return tuple(sorted(values))


def _checked_capabilities(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CryptoError("capabilities must be a canonical array")
    canonical = canonical_capabilities(value)
    if value != list(canonical):
        raise CryptoError("capabilities are not sorted and unique")
    return canonical


@dataclass(frozen=True)
class CapabilityOffer:
    capabilities: tuple[str, ...]
    required: tuple[str, ...]


def capability_offer(capabilities: Iterable[str] = DEFAULT_CAPABILITIES,
                     required: Iterable[str] = DEFAULT_REQUIRED) -> CapabilityOffer:
    supported = canonical_capabilities(capabilities)
    required = canonical_capabilities(required)
    if not set(required).issubset(supported):
        raise CryptoError("requirements are not a subset of capabilities")
    return CapabilityOffer(supported, required)


def offer_bytes(offer: CapabilityOffer) -> bytes:
    """Encode the canonical plaintext carried by initiator Noise message 1."""
    checked = capability_offer(offer.capabilities, offer.required)
    return dumps({
        "v": 1,
        "type": "netz/noise/offer",
        "capabilities": list(checked.capabilities),
        "required": list(checked.required),
    })


def parse_offer(payload: bytes) -> CapabilityOffer:
    """Decode and strictly validate the canonical message-1 offer."""
    try:
        value = loads(payload)
    except Exception as error:
        raise CryptoError("invalid Noise offer encoding") from error
    expected = {"v", "type", "capabilities", "required"}
    if (not isinstance(value, dict) or set(value) != expected
            or value.get("v") != 1 or value.get("type") != "netz/noise/offer"):
        raise CryptoError("malformed Noise offer")
    supported = _checked_capabilities(value["capabilities"])
    required = _checked_capabilities(value["required"])
    return capability_offer(supported, required)


def selected_capabilities(local: CapabilityOffer,
                          remote: CapabilityOffer) -> tuple[str, ...]:
    """Return the exact intersection or reject missing requirements."""
    selected = tuple(sorted(set(local.capabilities).intersection(remote.capabilities)))
    selected_set = set(selected)
    if (not selected
            or not set(local.required).issubset(selected_set)
            or not set(remote.required).issubset(selected_set)):
        raise CryptoError("no capability selection satisfies both peers")
    return selected


def sign_identity_record(genesis: dict[str, object], private_key: bytes,
                         addresses: Iterable[str], sequence: int) -> dict[str, object]:
    """Construct the exact signed identity record in the Noise payload."""
    try:
        genesis_bytes(genesis)
        operational_keys = genesis["operational"]
    except (IdentityError, KeyError, TypeError) as error:
        raise CryptoError("invalid genesis object") from error
    public_key = _public_bytes(private_key)
    if public_key not in operational_keys:
        raise CryptoError("signing key is not authorized by genesis")
    addresses = list(addresses)
    if len(addresses) > MAX_ADDRESSES:
        raise CryptoError("invalid identity addresses")
    addresses = sorted(normalize_websocket_uri(address) for address in addresses)
    if type(sequence) is not int or not 0 <= sequence <= MAX_SAFE_INTEGER:
        raise CryptoError("identity sequence is outside the safe integer range")
    transcript = identity_record_bytes(genesis, public_key, addresses, sequence)
    signature = (_ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
                 .sign(transcript))
    return {
        "v": 1,
        "genesis": genesis,
        "authority": authority_id(genesis),
        "operational": public_key,
        "addresses": addresses,
        "seq": sequence,
        "signature": signature,
    }


def verify_identity_record(record: Mapping[str, object]) -> bytes:
    """Verify an exact-schema record and return its full Authority ID."""
    expected = {
        "v", "genesis", "authority", "operational", "addresses", "seq",
        "signature",
    }
    if not isinstance(record, dict) or set(record) != expected or record.get("v") != 1:
        raise CryptoError("invalid identity record schema")
    genesis = record["genesis"]
    claimed_authority = record["authority"]
    operational = record["operational"]
    addresses = record["addresses"]
    sequence = record["seq"]
    signature = record["signature"]
    if (not isinstance(genesis, dict)
            or not isinstance(claimed_authority, bytes) or len(claimed_authority) != KEY_BYTES
            or not isinstance(operational, bytes) or len(operational) != KEY_BYTES
            or not isinstance(addresses, list) or len(addresses) > MAX_ADDRESSES
            or any(type(address) is not str for address in addresses)
            or addresses != sorted(addresses)
            or type(sequence) is not int or not 0 <= sequence <= MAX_SAFE_INTEGER
            or not isinstance(signature, bytes) or len(signature) != SIGNATURE_BYTES):
        raise CryptoError("invalid identity record fields")
    try:
        for address in addresses:
            _validate_signed_websocket_uri(address)
    except CryptoError as error:
        raise CryptoError("invalid identity record fields") from error
    try:
        genesis_bytes(genesis)
    except IdentityError as error:
        raise CryptoError("invalid genesis object") from error
    derived_authority = authority_id(genesis)
    if claimed_authority != derived_authority:
        raise CryptoError("identity record Authority ID does not match genesis")
    if operational not in genesis["operational"]:
        raise CryptoError("record key is not authorized by genesis")
    transcript = identity_record_bytes(genesis, operational, addresses, sequence)
    try:
        _ed25519().Ed25519PublicKey.from_public_bytes(operational).verify(
            signature, transcript
        )
    except Exception as error:
        raise CryptoError("invalid identity record signature") from error
    return derived_authority


def noise_static_binding_bytes(authority: bytes, noise_static: bytes, role: str,
                               capabilities: Iterable[str], required: Iterable[str],
                               selected: Iterable[str]) -> bytes:
    """Return the normative operational-key binding transcript."""
    if (not isinstance(authority, bytes) or len(authority) != KEY_BYTES
            or not isinstance(noise_static, bytes) or len(noise_static) != KEY_BYTES
            or role not in ROLES):
        raise CryptoError("invalid Authority ID, Noise static key, or role")
    capabilities = canonical_capabilities(capabilities)
    required = canonical_capabilities(required)
    selected = canonical_capabilities(selected)
    return NOISE_STATIC_DOMAIN + dumps({
        "v": 1,
        "protocol": NOISE_PROTOCOL_NAME.decode("ascii"),
        "authority": authority,
        "noise_static": noise_static,
        "role": role,
        "capabilities": list(capabilities),
        "required": list(required),
        "selected": list(selected),
    })


@dataclass(frozen=True)
class VerifiedPeer:
    """Authenticated peer context returned by the secure transport."""

    authority: bytes
    routing_id: bytes
    operational: bytes
    noise_static: bytes
    addresses: tuple[str, ...]
    sequence: int
    capabilities: tuple[str, ...]
    supported: tuple[str, ...]
    required: tuple[str, ...]
    role: str


def make_identity_payload(record: dict[str, object], private_key: bytes,
                          noise_static: bytes, role: str,
                          capabilities: Iterable[str], required: Iterable[str],
                          selected: Iterable[str]) -> bytes:
    """Encode the normative identity payload for XX message 2 or 3."""
    authority = verify_identity_record(record)
    if _public_bytes(private_key) != record["operational"]:
        raise CryptoError("payload signing key differs from identity record")
    offer = capability_offer(capabilities, required)
    selected = canonical_capabilities(selected)
    signature = (_ed25519().Ed25519PrivateKey.from_private_bytes(private_key)
                 .sign(noise_static_binding_bytes(
                     authority, noise_static, role, offer.capabilities,
                     offer.required, selected,
                 )))
    return dumps({
        "v": 1,
        "type": "netz/noise/identity",
        "record": record,
        "noise_static": noise_static,
        "role": role,
        "capabilities": list(offer.capabilities),
        "required": list(offer.required),
        "selected": list(selected),
        "signature": signature,
    })


def verify_identity_payload(payload: bytes, actual_noise_static: bytes, *,
                            expected_role: str, local_offer: CapabilityOffer,
                            expected_offer: CapabilityOffer | None = None,
                            expected_authority: bytes | None = None) -> VerifiedPeer:
    """Verify identity, target, transcript offer, and exact intersection."""
    try:
        value = loads(payload)
    except Exception as error:
        raise CryptoError("invalid identity payload encoding") from error
    expected = {
        "v", "type", "record", "noise_static", "role", "capabilities",
        "required", "selected", "signature",
    }
    if (not isinstance(value, dict) or set(value) != expected
            or value.get("v") != 1
            or value.get("type") != "netz/noise/identity"
            or value.get("role") != expected_role
            or not isinstance(value.get("noise_static"), bytes)
            or len(value["noise_static"]) != KEY_BYTES
            or value["noise_static"] != actual_noise_static
            or not isinstance(value.get("signature"), bytes)
            or len(value["signature"]) != SIGNATURE_BYTES):
        raise CryptoError("malformed or mismatched Noise identity payload")
    record = value["record"]
    if not isinstance(record, dict):
        raise CryptoError("malformed identity record")
    remote_offer = capability_offer(
        _checked_capabilities(value["capabilities"]),
        _checked_capabilities(value["required"]),
    )
    if expected_offer is not None and remote_offer != expected_offer:
        raise CryptoError("identity capabilities differ from the transcript offer")
    selected = _checked_capabilities(value["selected"])
    exact_selected = selected_capabilities(local_offer, remote_offer)
    if selected != exact_selected:
        raise CryptoError("peer selected a downgraded capability set")
    authority = verify_identity_record(record)
    if expected_authority is not None:
        if not isinstance(expected_authority, bytes) or len(expected_authority) != KEY_BYTES:
            raise CryptoError("expected target must be a full 32-byte Authority ID")
        if authority != expected_authority:
            raise CryptoError("remote authority differs from expected dial target")
    try:
        _ed25519().Ed25519PublicKey.from_public_bytes(record["operational"]).verify(
            value["signature"],
            noise_static_binding_bytes(
                authority, actual_noise_static, expected_role,
                remote_offer.capabilities, remote_offer.required, selected,
            ),
        )
    except Exception as error:
        raise CryptoError("invalid Noise static-key binding") from error
    return VerifiedPeer(
        authority=authority,
        routing_id=authority[:16],
        operational=record["operational"],
        noise_static=actual_noise_static,
        addresses=tuple(record["addresses"]),
        sequence=record["seq"],
        capabilities=selected,
        supported=remote_offer.capabilities,
        required=remote_offer.required,
        role=expected_role,
    )


__all__ = [
    "CapabilityOffer",
    "CryptoError",
    "DEFAULT_CAPABILITIES",
    "DEFAULT_REQUIRED",
    "MAX_ADDRESSES",
    "MAX_CAPABILITIES",
    "NOISE_PROLOGUE",
    "NOISE_PROTOCOL_NAME",
    "NOISE_STATIC_DOMAIN",
    "VerifiedPeer",
    "canonical_capabilities",
    "capability_offer",
    "make_identity_payload",
    "noise_static_binding_bytes",
    "normalize_websocket_uri",
    "offer_bytes",
    "parse_offer",
    "selected_capabilities",
    "sign_identity_record",
    "verify_identity_payload",
    "verify_identity_record",
]
