"""Experimental optional Noise XX adapter for the Python reference client.

The dependency-free protocol core does not import this module.  The adapter is
deliberately small so it can be replaced by cross-language Geheimnis bindings
without changing the public identity payload or carrier framing.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from typing import Any, Iterable

from netz_codec import MAX_FRAME_BYTES

from .crypto import (
    CryptoError,
    NOISE_PROLOGUE,
    NOISE_PROTOCOL_NAME,
    VerifiedPeer,
    make_identity_payload,
    sign_identity_record,
    verify_identity_payload,
)

NOISE_TAG_BYTES = 16
NOISE_MAX_CIPHERTEXT = 65535
NOISE_MAX_PLAINTEXT = NOISE_MAX_CIPHERTEXT - NOISE_TAG_BYTES
LENGTH_PREFIX_BYTES = 4


class SecureTransportError(ConnectionError):
    """The secure handshake or encrypted ordered carrier failed."""


def _noise_api():
    try:
        from noise.connection import Keypair, NoiseConnection
    except ImportError as error:  # pragma: no cover - depends on installation
        raise RuntimeError(
            "Noise sessions require 'netz-protocol[secure-client]'"
        ) from error
    return Keypair, NoiseConnection


def _x25519_public(private_key: bytes) -> bytes:
    if not isinstance(private_key, bytes) or len(private_key) != 32:
        raise SecureTransportError("Noise static private key must be 32 bytes")
    try:
        from cryptography.hazmat.primitives.asymmetric import x25519
    except ImportError as error:  # pragma: no cover - depends on installation
        raise RuntimeError(
            "Noise sessions require 'netz-protocol[secure-client]'"
        ) from error
    return x25519.X25519PrivateKey.from_private_bytes(private_key).public_key().public_bytes_raw()


@dataclass(frozen=True)
class LocalIdentity:
    """Inputs required to construct a fresh per-connection Noise identity."""

    genesis: dict[str, object]
    operational_private: bytes
    addresses: tuple[str, ...] = ()
    sequence: int = 0
    capabilities: tuple[str, ...] = ()

    def payload(self, noise_static: bytes) -> bytes:
        record = sign_identity_record(
            self.genesis, self.operational_private, self.addresses, self.sequence
        )
        return make_identity_payload(
            record, self.operational_private, noise_static, self.capabilities
        )


class NoiseXXSession:
    """One ordered ``Noise_XX_25519_AESGCM_SHA256`` connection."""

    def __init__(self, *, initiator: bool, static_private: bytes,
                 local_identity: LocalIdentity,
                 expected_authority: bytes | None = None) -> None:
        Keypair, NoiseConnection = _noise_api()
        self.initiator = initiator
        self.expected_authority = expected_authority
        self.remote_peer: VerifiedPeer | None = None
        self.closed = False
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
        self._local_payload = local_identity.payload(self.local_static)

        # noiseprotocol 0.3.1 does not expose the authenticated remote static
        # after XX completes. Capture it immediately before its cleanup hook.
        # Pinning 0.3.1 contains this private-API dependency; an upstream API is
        # required before calling this adapter production-ready.
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

    def _current_remote_static(self) -> bytes:
        if self._remote_static is not None:
            return self._remote_static
        try:
            return bytes(self._noise.noise_protocol.handshake_state.rs.public_bytes)
        except Exception as error:
            raise SecureTransportError("Noise did not authenticate a remote static key") from error

    def write_handshake(self) -> bytes:
        if self.closed or self.handshake_finished:
            raise SecureTransportError("handshake is unavailable")
        # XX: initiator's first payload is not encrypted and cannot yet be
        # bound to an authenticated static, so profile 1 requires it empty.
        payload = self._local_payload if (
            (self.initiator and self._step == 2)
            or (not self.initiator and self._step == 1)
        ) else b""
        try:
            message = bytes(self._noise.write_message(payload))
            if len(message) > NOISE_MAX_CIPHERTEXT:
                raise SecureTransportError("Noise handshake frame exceeds 65,535 bytes")
        except Exception as error:
            self.closed = True
            raise SecureTransportError("failed to write Noise handshake message") from error
        self._step += 1
        return message

    def read_handshake(self, message: bytes) -> VerifiedPeer | None:
        if self.closed or self.handshake_finished:
            raise SecureTransportError("handshake is unavailable")
        if not isinstance(message, bytes) or len(message) > NOISE_MAX_CIPHERTEXT:
            self.closed = True
            raise SecureTransportError("invalid Noise handshake frame")
        try:
            payload = bytes(self._noise.read_message(message))
            self._step += 1
            if payload:
                self.remote_peer = verify_identity_payload(
                    payload, self._current_remote_static(), self.expected_authority
                )
            elif self._step != 1 or self.initiator:
                raise CryptoError("authenticated XX identity payload is missing")
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
    """Length-delimited inner frames over Noise transport WebSocket messages."""

    def __init__(self, websocket: Any, session: NoiseXXSession) -> None:
        if not session.handshake_finished or session.remote_peer is None:
            raise SecureTransportError("cannot wrap an incomplete Noise session")
        self.websocket = websocket
        self.session = session
        self.peer = session.remote_peer
        self._send_lock = asyncio.Lock()
        self._recv_lock = asyncio.Lock()
        self._plain = bytearray()

    async def send_frame(self, frame: bytes) -> None:
        if not isinstance(frame, bytes) or len(frame) > MAX_FRAME_BYTES + 4:
            raise SecureTransportError("inner frame exceeds the Netz limit")
        stream = len(frame).to_bytes(LENGTH_PREFIX_BYTES, "big") + frame
        async with self._send_lock:
            for offset in range(0, len(stream), NOISE_MAX_PLAINTEXT):
                await self.websocket.send(
                    self.session.encrypt(stream[offset:offset + NOISE_MAX_PLAINTEXT])
                )

    async def recv_frame(self) -> bytes:
        async with self._recv_lock:
            while True:
                if len(self._plain) >= LENGTH_PREFIX_BYTES:
                    size = int.from_bytes(self._plain[:LENGTH_PREFIX_BYTES], "big")
                    if size > MAX_FRAME_BYTES + 4:
                        raise SecureTransportError("announced inner frame exceeds the Netz limit")
                    end = LENGTH_PREFIX_BYTES + size
                    if len(self._plain) >= end:
                        frame = bytes(self._plain[LENGTH_PREFIX_BYTES:end])
                        del self._plain[:end]
                        return frame
                message = await self.websocket.recv()
                if not isinstance(message, bytes):
                    raise SecureTransportError("secure carrier requires binary WebSocket messages")
                self._plain.extend(self.session.decrypt(message))


async def open_secure_websocket(uri: str, *, identity: LocalIdentity,
                                expected_authority: bytes,
                                static_private: bytes | None = None,
                                **connect_options: Any) -> SecureWebSocket:
    """Dial, complete XX, and return a minimal encrypted frame carrier."""
    try:
        import os
        from websockets.asyncio.client import connect
    except ImportError as error:  # pragma: no cover - depends on installation
        raise RuntimeError(
            "WebSocket transport requires 'netz-protocol[secure-client]'"
        ) from error
    static_private = static_private or os.urandom(32)
    connect_options.setdefault("max_size", NOISE_MAX_CIPHERTEXT)
    websocket = await connect(uri, **connect_options)
    session = NoiseXXSession(
        initiator=True,
        static_private=static_private,
        local_identity=identity,
        expected_authority=expected_authority,
    )
    try:
        await websocket.send(session.write_handshake())
        response = await websocket.recv()
        if not isinstance(response, bytes):
            raise SecureTransportError("Noise handshake requires binary messages")
        session.read_handshake(response)
        await websocket.send(session.write_handshake())
        return SecureWebSocket(websocket, session)
    except BaseException:
        await websocket.close()
        raise


async def accept_secure_websocket(websocket: Any, *, identity: LocalIdentity,
                                  static_private: bytes | None = None) -> SecureWebSocket:
    """Complete the responder side on an already accepted WebSocket."""
    import os
    static_private = static_private or os.urandom(32)
    session = NoiseXXSession(
        initiator=False,
        static_private=static_private,
        local_identity=identity,
    )
    try:
        request = await websocket.recv()
        if not isinstance(request, bytes):
            raise SecureTransportError("Noise handshake requires binary messages")
        session.read_handshake(request)
        await websocket.send(session.write_handshake())
        final = await websocket.recv()
        if not isinstance(final, bytes):
            raise SecureTransportError("Noise handshake requires binary messages")
        session.read_handshake(final)
        return SecureWebSocket(websocket, session)
    except BaseException:
        await websocket.close()
        raise


__all__ = [
    "LENGTH_PREFIX_BYTES",
    "LocalIdentity",
    "NOISE_MAX_CIPHERTEXT",
    "NOISE_MAX_PLAINTEXT",
    "NoiseXXSession",
    "SecureTransportError",
    "SecureWebSocket",
    "accept_secure_websocket",
    "open_secure_websocket",
]
