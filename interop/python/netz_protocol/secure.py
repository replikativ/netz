"""Experimental authenticated Noise/Kabel WebSocket binding.

The dependency-free protocol core does not import this module. This adapter
implements the direct carrier defined by Netz draft-00 and keeps the optional
Python networking and crypto dependencies behind ``[secure-client]``.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from typing import Any

from netz_codec import MAX_FRAME_BYTES

from .crypto import (
    CapabilityOffer,
    CryptoError,
    DEFAULT_CAPABILITIES,
    DEFAULT_REQUIRED,
    NOISE_PROLOGUE,
    NOISE_PROTOCOL_NAME,
    VerifiedPeer,
    capability_offer,
    make_identity_payload,
    offer_bytes,
    parse_offer,
    selected_capabilities,
    sign_identity_record,
    verify_identity_payload,
)

from .framing import KABEL_CBOR_SERIALIZER_ID, KABEL_PREFIX_BYTES

NOISE_TAG_BYTES = 16
NOISE_MAX_CIPHERTEXT = 65535
NOISE_MAX_PLAINTEXT = NOISE_MAX_CIPHERTEXT - NOISE_TAG_BYTES

KABEL_BINARY_SERIALIZER_ID = 0
HANDSHAKE_KIND = 1
TRANSPORT_KIND = 2
FRAGMENT_SINGLE = 0
FRAGMENT_START = 1
FRAGMENT_MIDDLE = 2
FRAGMENT_END = 3
FRAGMENT_DATA_LIMIT = NOISE_MAX_PLAINTEXT - 1
MAX_ENVELOPE_BYTES = MAX_FRAME_BYTES
MAX_WEBSOCKET_MESSAGE = KABEL_PREFIX_BYTES + 1 + NOISE_MAX_CIPHERTEXT


class SecureTransportError(ConnectionError):
    """The secure handshake, negotiation, or ordered carrier failed."""


def _noise_api():
    try:
        from noise.connection import Keypair, NoiseConnection
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError(
            "Noise sessions require 'netz-protocol[secure-client]'"
        ) from error
    return Keypair, NoiseConnection


def _x25519_public(private_key: bytes) -> bytes:
    if not isinstance(private_key, bytes) or len(private_key) != 32:
        raise SecureTransportError("Noise static private key must be 32 bytes")
    try:
        from cryptography.hazmat.primitives.asymmetric import x25519
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError(
            "Noise sessions require 'netz-protocol[secure-client]'"
        ) from error
    return (x25519.X25519PrivateKey.from_private_bytes(private_key)
            .public_key().public_bytes_raw())


def _wrap_outer(kind: int, payload: bytes) -> bytes:
    """Build serializer-0 Kabel bytes containing one visible carrier kind."""
    if kind not in (HANDSHAKE_KIND, TRANSPORT_KIND) or not isinstance(payload, bytes):
        raise SecureTransportError("invalid secure outer frame")
    return (KABEL_BINARY_SERIALIZER_ID.to_bytes(KABEL_PREFIX_BYTES, "big")
            + bytes([kind]) + payload)


def _unwrap_outer(frame: bytes, expected_kind: int) -> bytes:
    if not isinstance(frame, bytes) or len(frame) < KABEL_PREFIX_BYTES + 1:
        raise SecureTransportError("truncated secure Kabel frame")
    serializer = int.from_bytes(frame[:KABEL_PREFIX_BYTES], "big")
    kind = frame[KABEL_PREFIX_BYTES]
    payload = frame[KABEL_PREFIX_BYTES + 1:]
    if serializer != KABEL_BINARY_SERIALIZER_ID:
        raise SecureTransportError("secure outer frame must use Kabel serializer 0")
    if kind != expected_kind:
        raise SecureTransportError("unexpected secure carrier frame kind")
    if len(payload) > NOISE_MAX_CIPHERTEXT:
        raise SecureTransportError("Noise message exceeds 65,535 bytes")
    return payload


def _encode_envelope(payload: bytes) -> bytes:
    if not isinstance(payload, bytes):
        raise SecureTransportError("Kabel CBOR payload must be bytes")
    envelope = KABEL_CBOR_SERIALIZER_ID.to_bytes(KABEL_PREFIX_BYTES, "big") + payload
    if len(envelope) > MAX_ENVELOPE_BYTES:
        raise SecureTransportError("encoded Kabel envelope exceeds 5 MiB")
    return envelope


def _decode_envelope(envelope: bytes) -> bytes:
    if (not isinstance(envelope, bytes)
            or not KABEL_PREFIX_BYTES <= len(envelope) <= MAX_ENVELOPE_BYTES):
        raise SecureTransportError("invalid Kabel envelope length")
    serializer = int.from_bytes(envelope[:KABEL_PREFIX_BYTES], "big")
    if serializer != KABEL_CBOR_SERIALIZER_ID:
        raise SecureTransportError("peer sent an unnegotiated Kabel serializer")
    return envelope[KABEL_PREFIX_BYTES:]


@dataclass(frozen=True)
class LocalIdentity:
    """Inputs for the connection identity record and capability offer."""

    genesis: dict[str, object]
    operational_private: bytes
    addresses: tuple[str, ...] = ()
    sequence: int = 0
    capabilities: tuple[str, ...] = DEFAULT_CAPABILITIES
    required: tuple[str, ...] = DEFAULT_REQUIRED

    def record(self) -> dict[str, object]:
        return sign_identity_record(
            self.genesis, self.operational_private, self.addresses, self.sequence
        )

    def payload(self, noise_static: bytes, role: str,
                selected: tuple[str, ...]) -> bytes:
        return make_identity_payload(
            self.record(), self.operational_private, noise_static, role,
            self.capabilities, self.required, selected,
        )


class NoiseXXSession:
    """One negotiated ``Noise_XX_25519_AESGCM_SHA256`` connection."""

    def __init__(self, *, initiator: bool, static_private: bytes,
                 local_identity: LocalIdentity,
                 expected_authority: bytes | None = None) -> None:
        Keypair, NoiseConnection = _noise_api()
        if (expected_authority is not None
                and (not isinstance(expected_authority, bytes)
                     or len(expected_authority) != 32)):
            raise SecureTransportError(
                "expected target must be a full 32-byte Authority ID"
            )
        self.initiator = initiator
        self.role = "initiator" if initiator else "responder"
        self.remote_role = "responder" if initiator else "initiator"
        self.expected_authority = expected_authority
        self.remote_peer: VerifiedPeer | None = None
        self.closed = False
        self._identity = local_identity
        self._local_offer = capability_offer(
            local_identity.capabilities, local_identity.required
        )
        self._remote_offer: CapabilityOffer | None = None
        self._selected: tuple[str, ...] | None = None
        self._remote_static: bytes | None = None
        self._step = 0
        self._send_lock = threading.Lock()
        self._receive_lock = threading.Lock()
        self._noise = NoiseConnection.from_name(NOISE_PROTOCOL_NAME)
        self._noise.set_prologue(NOISE_PROLOGUE)
        (self._noise.set_as_initiator if initiator
         else self._noise.set_as_responder)()
        self._noise.set_keypair_from_private_bytes(Keypair.STATIC, static_private)
        self.local_static = _x25519_public(static_private)

        # noiseprotocol 0.3.1 deletes the authenticated remote XX static at
        # split without exposing it. Pin the package and capture that value in
        # one compatibility hook until an upstream accessor exists.
        protocol = self._noise.noise_protocol
        finish = protocol.handshake_done

        def capture_remote_static() -> None:
            remote = protocol.handshake_state.rs
            self._remote_static = bytes(remote.public_bytes)
            finish()

        protocol.handshake_done = capture_remote_static
        self._noise.start_handshake()

    @property
    def handshake_finished(self) -> bool:
        return bool(self._noise.handshake_finished)

    @property
    def handshake_hash(self) -> bytes:
        if not self.handshake_finished:
            raise SecureTransportError("handshake is incomplete")
        return self._noise.get_handshake_hash()

    @property
    def selected(self) -> tuple[str, ...]:
        if self._selected is None:
            raise SecureTransportError("capability negotiation is incomplete")
        return self._selected

    def _current_remote_static(self) -> bytes:
        if self._remote_static is not None:
            return self._remote_static
        try:
            return bytes(self._noise.noise_protocol.handshake_state.rs.public_bytes)
        except Exception as error:
            raise SecureTransportError(
                "Noise did not authenticate a remote static key"
            ) from error

    def write_handshake(self) -> bytes:
        if self.closed or self.handshake_finished:
            raise SecureTransportError("handshake is unavailable")
        try:
            if self.initiator and self._step == 0:
                payload = offer_bytes(self._local_offer)
            elif not self.initiator and self._step == 1:
                if self._selected is None:
                    raise CryptoError("initiator offer was not accepted")
                payload = self._identity.payload(
                    self.local_static, self.role, self._selected
                )
            elif self.initiator and self._step == 2:
                if self._selected is None or self.remote_peer is None:
                    raise CryptoError("responder identity was not verified")
                payload = self._identity.payload(
                    self.local_static, self.role, self._selected
                )
            else:
                raise CryptoError("Noise handshake write called out of turn")
            message = bytes(self._noise.write_message(payload))
            if len(message) > NOISE_MAX_CIPHERTEXT:
                raise SecureTransportError("Noise handshake frame exceeds 65,535 bytes")
            self._step += 1
            return message
        except Exception as error:
            self.closed = True
            raise SecureTransportError("failed to write Noise handshake message") from error

    def read_handshake(self, message: bytes) -> VerifiedPeer | None:
        if self.closed or self.handshake_finished:
            raise SecureTransportError("handshake is unavailable")
        if not isinstance(message, bytes) or len(message) > NOISE_MAX_CIPHERTEXT:
            self.closed = True
            raise SecureTransportError("invalid Noise handshake frame")
        try:
            payload = bytes(self._noise.read_message(message))
            self._step += 1
            if not self.initiator and self._step == 1:
                self._remote_offer = parse_offer(payload)
                self._selected = selected_capabilities(
                    self._local_offer, self._remote_offer
                )
            elif self.initiator and self._step == 2:
                self.remote_peer = verify_identity_payload(
                    payload, self._current_remote_static(),
                    expected_role=self.remote_role,
                    local_offer=self._local_offer,
                    expected_authority=self.expected_authority,
                )
                self._remote_offer = capability_offer(
                    self.remote_peer.supported, self.remote_peer.required
                )
                self._selected = self.remote_peer.capabilities
            elif not self.initiator and self._step == 3:
                if self._remote_offer is None:
                    raise CryptoError("initiator transcript offer is missing")
                self.remote_peer = verify_identity_payload(
                    payload, self._current_remote_static(),
                    expected_role=self.remote_role,
                    local_offer=self._local_offer,
                    expected_offer=self._remote_offer,
                    expected_authority=self.expected_authority,
                )
                self._selected = self.remote_peer.capabilities
            else:
                raise CryptoError("Noise handshake read called out of turn")
            if self.handshake_finished and self.remote_peer is None:
                raise CryptoError("handshake completed without a remote identity")
            return self.remote_peer
        except Exception as error:
            self.closed = True
            raise SecureTransportError("failed to verify Noise handshake message") from error

    def encrypt(self, plaintext: bytes) -> bytes:
        if self.closed or not self.handshake_finished:
            raise SecureTransportError("secure session is unavailable")
        if not isinstance(plaintext, bytes) or len(plaintext) > NOISE_MAX_PLAINTEXT:
            raise SecureTransportError("Noise plaintext exceeds 65,519 bytes")
        try:
            with self._send_lock:
                return self._noise.encrypt(plaintext)
        except Exception as error:
            self.closed = True
            raise SecureTransportError("Noise encryption failed") from error

    def decrypt(self, ciphertext: bytes) -> bytes:
        if self.closed or not self.handshake_finished:
            raise SecureTransportError("secure session is unavailable")
        if not isinstance(ciphertext, bytes) or len(ciphertext) > NOISE_MAX_CIPHERTEXT:
            self.closed = True
            raise SecureTransportError("invalid Noise ciphertext frame")
        try:
            with self._receive_lock:
                return self._noise.decrypt(ciphertext)
        except Exception as error:
            self.closed = True
            raise SecureTransportError("Noise authentication failed") from error


class SecureWebSocket:
    """Serializer-14 Kabel envelopes over fragmented Noise transport frames."""

    def __init__(self, websocket: Any, session: NoiseXXSession) -> None:
        if not session.handshake_finished or session.remote_peer is None:
            raise SecureTransportError("cannot wrap an incomplete Noise session")
        self.websocket = websocket
        self.session = session
        self.peer = session.remote_peer
        self._send_lock = asyncio.Lock()
        self._recv_lock = asyncio.Lock()
        self._assembly: bytearray | None = None

    async def _abort(self) -> None:
        self.session.closed = True
        try:
            await self.websocket.close()
        except Exception:
            pass

    async def send_frame(self, cbor_payload: bytes) -> None:
        """Send exact CBOR bytes as one logical Kabel serializer-14 envelope."""
        try:
            envelope = _encode_envelope(cbor_payload)
            total = len(envelope)
            async with self._send_lock:
                for offset in range(0, total, FRAGMENT_DATA_LIMIT):
                    end = min(total, offset + FRAGMENT_DATA_LIMIT)
                    if offset == 0 and end == total:
                        flag = FRAGMENT_SINGLE
                    elif offset == 0:
                        flag = FRAGMENT_START
                    elif end == total:
                        flag = FRAGMENT_END
                    else:
                        flag = FRAGMENT_MIDDLE
                    ciphertext = self.session.encrypt(
                        bytes([flag]) + envelope[offset:end]
                    )
                    await self.websocket.send(_wrap_outer(TRANSPORT_KIND, ciphertext))
        except BaseException:
            await self._abort()
            raise

    def _append(self, data: bytes) -> None:
        if self._assembly is None:
            self._assembly = bytearray()
        if len(self._assembly) + len(data) > MAX_ENVELOPE_BYTES:
            raise SecureTransportError("fragmented Kabel envelope exceeds 5 MiB")
        self._assembly.extend(data)

    async def recv_frame(self) -> bytes:
        """Receive one logical envelope and return its exact CBOR payload."""
        try:
            async with self._recv_lock:
                while True:
                    outer = await self.websocket.recv()
                    ciphertext = _unwrap_outer(outer, TRANSPORT_KIND)
                    plaintext = self.session.decrypt(ciphertext)
                    if not plaintext:
                        raise SecureTransportError("empty Noise transport fragment")
                    flag, data = plaintext[0], plaintext[1:]
                    if flag == FRAGMENT_SINGLE:
                        if self._assembly is not None:
                            raise SecureTransportError(
                                "single fragment interrupted an envelope"
                            )
                        return _decode_envelope(data)
                    if flag == FRAGMENT_START:
                        if self._assembly is not None:
                            raise SecureTransportError("nested envelope start")
                        self._assembly = bytearray()
                        self._append(data)
                        continue
                    if flag == FRAGMENT_MIDDLE:
                        if self._assembly is None:
                            raise SecureTransportError("middle fragment without start")
                        self._append(data)
                        continue
                    if flag == FRAGMENT_END:
                        if self._assembly is None:
                            raise SecureTransportError("end fragment without start")
                        self._append(data)
                        envelope = bytes(self._assembly)
                        self._assembly = None
                        return _decode_envelope(envelope)
                    raise SecureTransportError("unknown secure fragment kind")
        except BaseException:
            await self._abort()
            raise


async def _read_outer(websocket: Any, kind: int) -> bytes:
    frame = await websocket.recv()
    return _unwrap_outer(frame, kind)


async def _close_silently(websocket: Any) -> None:
    try:
        await websocket.close()
    except Exception:
        pass


async def open_secure_websocket(uri: str, *, identity: LocalIdentity,
                                expected_authority: bytes,
                                static_private: bytes | None = None,
                                handshake_timeout: float = 15.0,
                                **connect_options: Any) -> SecureWebSocket:
    """Dial, negotiate, authenticate, and return the encrypted Kabel carrier."""
    try:
        import os
        from websockets.asyncio.client import connect
    except ImportError as error:  # pragma: no cover - installation dependent
        raise RuntimeError(
            "WebSocket transport requires 'netz-protocol[secure-client]'"
        ) from error
    static_private = static_private or os.urandom(32)
    if not 0 < handshake_timeout <= 60:
        raise SecureTransportError(
            "Noise handshake timeout must be between 0 and 60 seconds"
        )
    session = NoiseXXSession(
        initiator=True,
        static_private=static_private,
        local_identity=identity,
        expected_authority=expected_authority,
    )
    connect_options.setdefault("max_size", MAX_WEBSOCKET_MESSAGE)
    websocket = await connect(uri, **connect_options)
    try:
        async def handshake() -> None:
            await websocket.send(
                _wrap_outer(HANDSHAKE_KIND, session.write_handshake())
            )
            session.read_handshake(await _read_outer(websocket, HANDSHAKE_KIND))
            await websocket.send(
                _wrap_outer(HANDSHAKE_KIND, session.write_handshake())
            )

        await asyncio.wait_for(handshake(), timeout=handshake_timeout)
        return SecureWebSocket(websocket, session)
    except asyncio.TimeoutError as error:
        session.closed = True
        await _close_silently(websocket)
        raise SecureTransportError("Noise handshake timed out") from error
    except BaseException:
        session.closed = True
        await _close_silently(websocket)
        raise


async def accept_secure_websocket(websocket: Any, *, identity: LocalIdentity,
                                  static_private: bytes | None = None,
                                  expected_authority: bytes | None = None,
                                  handshake_timeout: float = 15.0) -> SecureWebSocket:
    """Complete the responder profile on an accepted raw WebSocket."""
    import os
    static_private = static_private or os.urandom(32)
    if not 0 < handshake_timeout <= 60:
        raise SecureTransportError(
            "Noise handshake timeout must be between 0 and 60 seconds"
        )
    session = NoiseXXSession(
        initiator=False,
        static_private=static_private,
        local_identity=identity,
        expected_authority=expected_authority,
    )
    try:
        async def handshake() -> None:
            session.read_handshake(await _read_outer(websocket, HANDSHAKE_KIND))
            await websocket.send(
                _wrap_outer(HANDSHAKE_KIND, session.write_handshake())
            )
            session.read_handshake(await _read_outer(websocket, HANDSHAKE_KIND))

        await asyncio.wait_for(handshake(), timeout=handshake_timeout)
        return SecureWebSocket(websocket, session)
    except asyncio.TimeoutError as error:
        session.closed = True
        await _close_silently(websocket)
        raise SecureTransportError("Noise handshake timed out") from error
    except BaseException:
        session.closed = True
        await _close_silently(websocket)
        raise


__all__ = [
    "FRAGMENT_DATA_LIMIT",
    "FRAGMENT_END",
    "FRAGMENT_MIDDLE",
    "FRAGMENT_SINGLE",
    "FRAGMENT_START",
    "HANDSHAKE_KIND",
    "KABEL_BINARY_SERIALIZER_ID",
    "KABEL_CBOR_SERIALIZER_ID",
    "LocalIdentity",
    "MAX_ENVELOPE_BYTES",
    "NOISE_MAX_CIPHERTEXT",
    "NOISE_MAX_PLAINTEXT",
    "NoiseXXSession",
    "SecureTransportError",
    "SecureWebSocket",
    "TRANSPORT_KIND",
    "accept_secure_websocket",
    "open_secure_websocket",
]
