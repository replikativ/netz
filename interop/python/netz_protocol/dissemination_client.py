"""Reusable asyncio client for authenticated portable dissemination."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from typing import Any, cast

from .dissemination import (
    DISSEMINATION_CAPABILITY,
    Publisher,
    Stream,
    validate_frame,
)
from .dissemination_session import DisseminationSession
from .extension import (
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    decode_extension_frame,
    encode_extension_frame,
)
from .publication import verify_publication
from .secure import LocalIdentity, SecureWebSocket, open_secure_websocket

DISSEMINATION_CAPABILITIES = (
    WIRE_PROFILE_CAPABILITY,
    EXTENSION_CARRIER_CAPABILITY,
    DISSEMINATION_CAPABILITY,
)

Authorization = Callable[[Mapping[str, object]], bool | Awaitable[bool]]


class DisseminationClientError(ConnectionError):
    """The portable dissemination client failed closed."""


def dissemination_identity(identity: LocalIdentity) -> LocalIdentity:
    """Require the complete portable dissemination profile on an identity."""
    supported = tuple(sorted(set(identity.capabilities) |
                             set(DISSEMINATION_CAPABILITIES)))
    required = tuple(sorted(set(identity.required) |
                            set(DISSEMINATION_CAPABILITIES)))
    return replace(identity, capabilities=supported, required=required)


def _operation(frame: Mapping[str, object]) -> str:
    return {
        "gossip": "publish",
        "interests": "advertise-interests",
        "have": "advertise-history",
        "want": "request-history",
    }[cast(str, frame["type"])]


class DisseminationClient:
    """A reconnecting secure subscriber/publisher with bounded repair state.

    ``authorize`` is mandatory and runs for every inbound and outbound control
    or publication frame. It may return a boolean or awaitable; only exact
    ``True`` before the deadline permits disclosure or acceptance.
    """

    def __init__(self, *, identity: LocalIdentity, expected_authority: bytes,
                 topics: tuple[str, ...], authorize: Authorization,
                 epoch: int | None = None, publish: bool = False,
                 carries: tuple[str, ...] = (), relay_all: bool = False,
                 queue_size: int = 1024, authorization_timeout: float = 5.0,
                 have_interval: float = 5.0) -> None:
        if not callable(authorize):
            raise TypeError("authorize must be callable")
        if not isinstance(expected_authority, bytes) or len(expected_authority) != 32:
            raise ValueError("expected_authority must be a full 32-byte Authority ID")
        if queue_size < 1 or not 0 < authorization_timeout <= 60 \
                or not 0 < have_interval <= 3600:
            raise ValueError("invalid client queue or timeout bound")
        if publish and epoch is None:
            raise ValueError(
                "a publishing client requires an explicitly managed epoch")
        self.identity = dissemination_identity(identity)
        self.expected_authority = expected_authority
        self.authorize = authorize
        self.authorization_timeout = authorization_timeout
        self.have_interval = have_interval
        publisher = Publisher(identity.genesis, identity.operational_private,
                              epoch=cast(int, epoch)) if publish else None
        self.session = DisseminationSession(
            topics=topics, carries=carries, relay_all=relay_all,
            publisher=publisher,
        )
        self.publications: asyncio.Queue[dict[str, object]] = asyncio.Queue(queue_size)
        self.state_sync: asyncio.Queue[tuple[Stream, int, int]] = asyncio.Queue(
            queue_size)
        self.connected = asyncio.Event()
        self.last_error: BaseException | None = None
        self._socket: SecureWebSocket | None = None
        self._stop = asyncio.Event()

    async def _authorized(self, direction: str,
                          frame: Mapping[str, object]) -> None:
        context: dict[str, object] = {
            "op": _operation(frame),
            "direction": direction,
            "peer_authority": self.expected_authority,
            "frame_type": frame["type"],
            "frame": frame,
        }
        if frame["type"] == "gossip":
            context.update({
                "origin_authority": frame["origin"],
                "topic": frame["topic"],
                "payload": frame["payload"],
            })
        try:
            decision = self.authorize(context)
        except Exception as error:
            raise DisseminationClientError(
                "dissemination authorization failed") from error
        if inspect.isawaitable(decision):
            try:
                decision = await asyncio.wait_for(
                    cast(Awaitable[bool], decision),
                    timeout=self.authorization_timeout,
                )
            except asyncio.TimeoutError as error:
                raise DisseminationClientError(
                    "dissemination authorization timed out") from error
            except Exception as error:
                raise DisseminationClientError(
                    "dissemination authorization failed") from error
        if decision is not True:
            raise DisseminationClientError("dissemination authorization denied")

    async def _send(self, socket: SecureWebSocket,
                    frame: Mapping[str, object]) -> None:
        frame = validate_frame(frame)
        await self._authorized("outbound", frame)
        await socket.send_frame(encode_extension_frame(
            DISSEMINATION_CAPABILITY,
            frame,
            selected_capabilities=socket.peer.capabilities,
        ))

    async def _receive(self, socket: SecureWebSocket) -> None:
        while not self._stop.is_set():
            encoded = await socket.recv_frame()
            try:
                capability, value = decode_extension_frame(
                    encoded, selected_capabilities=socket.peer.capabilities)
                if capability != DISSEMINATION_CAPABILITY:
                    raise DisseminationClientError(
                        "unexpected extension on dissemination connection")
                frame = validate_frame(value)
                # Authenticate a claimed origin before policy is allowed to
                # rely on it. The session verifies again at its pure boundary.
                if frame["type"] == "gossip":
                    verify_publication(frame)
            except DisseminationClientError:
                raise
            except Exception as error:
                raise DisseminationClientError(
                    "invalid portable dissemination frame") from error
            await self._authorized("inbound", frame)
            result = self.session.receive(frame)
            for publication in result.delivered:
                await self.publications.put(publication)
            for required in result.state_sync_required:
                await self.state_sync.put(required)
            for outbound in result.outbound:
                await self._send(socket, outbound)

    async def _serve_connection(self, uri: str,
                                connect_options: Mapping[str, Any]) -> None:
        socket = await open_secure_websocket(
            uri,
            identity=self.identity,
            expected_authority=self.expected_authority,
            **dict(connect_options),
        )
        self._socket = socket
        receive: asyncio.Task[None] | None = None
        try:
            selected = set(socket.peer.capabilities)
            if not set(DISSEMINATION_CAPABILITIES).issubset(selected):
                raise DisseminationClientError(
                    "peer did not negotiate portable dissemination")
            for frame in self.session.connected():
                await self._send(socket, frame)
            self.connected.set()
            self.last_error = None
            receive = asyncio.create_task(self._receive(socket))
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(asyncio.shield(receive),
                                           timeout=self.have_interval)
                    break
                except asyncio.TimeoutError:
                    await self._send(socket, self.session.have())
            if receive.done():
                await receive
        finally:
            if receive is not None and not receive.done():
                receive.cancel()
                try:
                    await receive
                except BaseException:
                    pass
            self.connected.clear()
            self.session.disconnected()
            self._socket = None
            await socket.websocket.close()

    async def run(self, uri: str, *, reconnect: bool = True,
                  initial_backoff: float = 0.25, max_backoff: float = 15.0,
                  **connect_options: Any) -> None:
        """Connect until ``close``; retain session state across reconnects."""
        if not 0 < initial_backoff <= max_backoff <= 300:
            raise ValueError("invalid reconnect backoff")
        delay = initial_backoff
        while not self._stop.is_set():
            try:
                await self._serve_connection(uri, connect_options)
                delay = initial_backoff
            except asyncio.CancelledError:
                raise
            except DisseminationClientError as error:
                self.last_error = error
                self.connected.clear()
                self.session.disconnected()
                if self._stop.is_set():
                    return
                # Policy and protocol failures are not transient network
                # partitions. Reconnecting automatically would retry an
                # unauthorized disclosure or a malicious frame forever.
                raise
            except BaseException as error:
                self.last_error = error
                self.connected.clear()
                self.session.disconnected()
                if not reconnect or self._stop.is_set():
                    if not self._stop.is_set():
                        raise
                    return
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    delay = min(max_backoff, delay * 2)

    async def publish(self, topic: str, payload: bytes) -> dict[str, object]:
        """Sign locally now and send live when an interested peer is connected."""
        publication = self.session.publish(topic, payload)
        # An uninterested peer does not receive the live value; history remains
        # available to a later interested connection.
        if self.session.peer_interested(topic) and self._socket is not None:
            await self._send(self._socket, publication)
        return publication

    async def recv(self) -> dict[str, object]:
        """Wait for the next verified, authorized, ordered publication."""
        return await self.publications.get()

    async def recv_state_sync(self) -> tuple[Stream, int, int]:
        """Wait for a repair horizon or bounded-order overflow escalation."""
        return await self.state_sync.get()

    async def close(self) -> None:
        self._stop.set()
        if self._socket is not None:
            await self._socket.websocket.close()


__all__ = [
    "Authorization", "DISSEMINATION_CAPABILITIES", "DisseminationClient",
    "DisseminationClientError", "dissemination_identity",
]
