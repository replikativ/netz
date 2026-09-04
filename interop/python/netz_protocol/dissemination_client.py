"""Reusable asyncio client for authenticated portable dissemination."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
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
from .frontier_store import FrontierStore, MemoryFrontierStore, encode_frontiers
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


@dataclass(frozen=True)
class Delivery:
    """One ordered publication whose application commit is still explicit."""

    publication: dict[str, object]
    _stream: Stream = field(repr=False, compare=False)
    _sequence: int = field(repr=False, compare=False)
    _commit: Callable[[Stream, int], Awaitable[None]] = field(
        repr=False, compare=False)

    async def commit(self) -> None:
        """Persist this stream frontier after the application value is safe."""
        await self._commit(self._stream, self._sequence)


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
                 have_interval: float = 5.0,
                 frontier_store: FrontierStore | None = None) -> None:
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
        self.frontier_store = (frontier_store if frontier_store is not None
                               else MemoryFrontierStore())
        if not isinstance(self.frontier_store, FrontierStore):
            raise TypeError("frontier_store must implement the async store protocol")
        self.publications: asyncio.Queue[Delivery] = asyncio.Queue(queue_size)
        self.state_sync: asyncio.Queue[tuple[Stream, int, int]] = asyncio.Queue(
            queue_size)
        self.connected = asyncio.Event()
        self.last_error: BaseException | None = None
        self._socket: SecureWebSocket | None = None
        self._stop = asyncio.Event()
        self._commit_lock = asyncio.Lock()
        self._committed: dict[Stream, int] = {}
        self._restored = False
        self._running = False

    async def _restore(self) -> None:
        async with self._commit_lock:
            if self._restored:
                return
            try:
                restored = dict(await self.frontier_store.load_frontiers())
                # Validate the complete record even for third-party stores.
                encode_frontiers(restored)
                self.session.receiver.commit_frontiers(restored)
            except Exception as error:
                raise DisseminationClientError(
                    "committed dissemination frontiers could not be restored") from error
            self._committed = restored
            self._restored = True

    def _delivery(self, publication: Mapping[str, object]) -> Delivery:
        checked = dict(publication)
        return Delivery(
            checked,
            self.session.receiver.stream(checked),
            cast(int, checked["seq"]),
            self._commit_delivery,
        )

    async def _commit_delivery(self, stream: Stream, sequence: int) -> None:
        await self._restore()
        async with self._commit_lock:
            through = self._committed.get(stream, -1)
            if sequence <= through:
                return
            if sequence != through + 1:
                raise DisseminationClientError(
                    "publication commits must be contiguous per stream")
            updated = {**self._committed, stream: sequence}
            try:
                await self.frontier_store.save_frontiers(updated)
            except Exception as error:
                raise DisseminationClientError(
                    "committed dissemination frontier was not persisted") from error
            self._committed = updated

    async def commit_frontiers(self, frontiers: Mapping[Stream, int]) -> None:
        """Persist application state-sync frontiers before releasing its suffix."""
        await self._restore()
        async with self._commit_lock:
            try:
                candidates = dict(frontiers)
                encode_frontiers({**self._committed, **candidates})
                if any(through < self._committed.get(stream, -1)
                       for stream, through in candidates.items()):
                    raise ValueError("state-sync frontier regressed")
                advances = {
                    stream: through for stream, through in candidates.items()
                    if through > self._committed.get(stream, -1)
                }
                if not advances:
                    return
                updated = {**self._committed, **advances}
                await self.frontier_store.save_frontiers(updated)
            except Exception as error:
                raise DisseminationClientError(
                    "state-sync dissemination frontiers could not be committed") from error
            received = self.session.receiver.commit_frontiers(advances)
            self._committed = updated
        for publication in received.delivered:
            await self.publications.put(self._delivery(publication))

    @property
    def committed_frontiers(self) -> Mapping[Stream, int]:
        return dict(self._committed)

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
                await self.publications.put(self._delivery(publication))
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
        if self._running:
            raise RuntimeError("dissemination client is already running")
        self._running = True
        try:
            await self._restore()
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
        finally:
            self._running = False

    async def publish(self, topic: str, payload: bytes) -> dict[str, object]:
        """Sign locally now and send live when an interested peer is connected."""
        publication = self.session.publish(topic, payload)
        # An uninterested peer does not receive the live value; history remains
        # available to a later interested connection.
        if self.session.peer_interested(topic) and self._socket is not None:
            await self._send(self._socket, publication)
        return publication

    async def recv(self) -> Delivery:
        """Wait for a verified ordered delivery requiring explicit commit."""
        while True:
            delivery = await self.publications.get()
            if delivery._sequence > self._committed.get(delivery._stream, -1):
                return delivery

    async def recv_state_sync(self) -> tuple[Stream, int, int]:
        """Wait for a repair horizon or bounded-order overflow escalation."""
        return await self.state_sync.get()

    async def close(self) -> None:
        self._stop.set()
        if self._socket is not None:
            await self._socket.websocket.close()


__all__ = [
    "Authorization", "DISSEMINATION_CAPABILITIES", "Delivery",
    "DisseminationClient", "DisseminationClientError", "dissemination_identity",
]
