"""Reusable asyncio client for authenticated dissemination, sync and content."""

from __future__ import annotations

import asyncio
import inspect
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, cast

from .content import (
    CONTENT_CAPABILITY,
    validate_frame as validate_content_frame,
)
from .content_session import (
    ContentRequester,
    ContentResult,
    ContentService,
)

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
from .sync import (
    SYNC_CAPABILITY,
    SyncProfile,
    SyncReceiver,
    SyncSender,
    open_frame,
)

DISSEMINATION_CAPABILITIES = (
    WIRE_PROFILE_CAPABILITY,
    EXTENSION_CARRIER_CAPABILITY,
    DISSEMINATION_CAPABILITY,
)
CLIENT_CAPABILITIES = DISSEMINATION_CAPABILITIES + (
    CONTENT_CAPABILITY, SYNC_CAPABILITY,
)

Authorization = Callable[[Mapping[str, object]], bool | Awaitable[bool]]
SnapshotProvider = Callable[
    [Mapping[str, object]], "SyncSnapshot | Awaitable[SyncSnapshot]"
]


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


@dataclass(frozen=True)
class SyncPullResult:
    """Application-committed state and locally accepted live frontiers."""

    value: Any
    frontiers: Mapping[Stream, int]


@dataclass(frozen=True)
class SyncSnapshot:
    """One authorized, internally consistent state-sync source snapshot."""

    value: Any
    frontiers: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class SyncServeResult:
    """Terminal outcome for one authorized incoming sync session."""

    session: bytes
    topic: str
    profile: str
    status: str


@dataclass(frozen=True)
class SyncSource:
    """A fixed topic/profile route whose snapshot is loaded after policy."""

    topic: str
    profile: SyncProfile
    snapshot: SnapshotProvider

    def __post_init__(self) -> None:
        if not isinstance(self.topic, str) or not self.topic:
            raise ValueError("sync source topic must be non-empty text")
        if (not isinstance(getattr(self.profile, "name", None), str)
                or not self.profile.name or not callable(self.snapshot)):
            raise TypeError("sync source requires a named profile and callback")


def dissemination_identity(identity: LocalIdentity, *,
                           require_dissemination: bool = True) -> LocalIdentity:
    """Offer content, sync and dissemination; optionally require dissemination."""
    supported = tuple(sorted(set(identity.capabilities) |
                             set(CLIENT_CAPABILITIES)))
    required_capabilities = (set(DISSEMINATION_CAPABILITIES)
                             if require_dissemination else set())
    required = tuple(sorted(set(identity.required) | required_capabilities))
    return replace(identity, capabilities=supported, required=required)


def _operation(frame: Mapping[str, object]) -> str:
    return {
        "gossip": "publish",
        "interests": "advertise-interests",
        "have": "advertise-history",
        "want": "request-history",
    }[cast(str, frame["type"])]


class DisseminationClient:
    """A reconnecting secure client with bounded dissemination, sync and content.

    ``authorize`` is mandatory and runs for every inbound and outbound
    dissemination, synchronization, or content frame. It may return a boolean or
    awaitable; only exact ``True`` before the deadline permits disclosure or
    acceptance.
    """

    def __init__(self, *, identity: LocalIdentity, expected_authority: bytes,
                 topics: tuple[str, ...], authorize: Authorization,
                 epoch: int | None = None, publish: bool = False,
                 carries: tuple[str, ...] = (), relay_all: bool = False,
                 queue_size: int = 1024, authorization_timeout: float = 5.0,
                 have_interval: float = 5.0,
                 frontier_store: FrontierStore | None = None,
                 sync_timeout: float = 30.0,
                 max_sync_sessions: int = 16,
                 require_dissemination: bool = True,
                 sync_sources: tuple[SyncSource, ...] = (),
                 max_sync_sources: int = 64,
                 content_service: ContentService | None = None,
                 content_timeout_ms: int = 5_000,
                 max_content_requests: int = 16,
                 max_content_service_requests: int = 16) -> None:
        if not callable(authorize):
            raise TypeError("authorize must be callable")
        if not isinstance(expected_authority, bytes) or len(expected_authority) != 32:
            raise ValueError("expected_authority must be a full 32-byte Authority ID")
        if queue_size < 1 or not 0 < authorization_timeout <= 60 \
                or not 0 < have_interval <= 3600:
            raise ValueError("invalid client queue or timeout bound")
        if (not 0 < sync_timeout <= 3600
                or not 0 < max_sync_sessions <= 256
                or not 0 < max_sync_sources <= 1024):
            raise ValueError("invalid synchronization timeout or session bound")
        if not isinstance(sync_sources, tuple) or len(sync_sources) > max_sync_sources:
            raise ValueError("invalid or excessive synchronization sources")
        if publish and epoch is None:
            raise ValueError(
                "a publishing client requires an explicitly managed epoch")
        if type(require_dissemination) is not bool:
            raise TypeError("require_dissemination must be boolean")
        if content_service is not None and not isinstance(
                content_service, ContentService):
            raise TypeError("content_service must be a ContentService")
        if not 1 <= max_content_service_requests <= 256:
            raise ValueError("invalid content service request bound")
        self.identity = dissemination_identity(
            identity, require_dissemination=require_dissemination)
        self.require_dissemination = require_dissemination
        self.expected_authority = expected_authority
        self.authorize = authorize
        self.authorization_timeout = authorization_timeout
        self.have_interval = have_interval
        self.sync_timeout = sync_timeout
        self.max_sync_sessions = max_sync_sessions
        self.content_service = content_service
        self.max_content_service_requests = max_content_service_requests
        self.content = ContentRequester(
            timeout_ms=content_timeout_ms,
            max_outstanding=max_content_requests,
        )
        self._sync_sources: dict[tuple[str, str], SyncSource] = {}
        for source in sync_sources:
            if not isinstance(source, SyncSource):
                raise TypeError("sync_sources must contain SyncSource values")
            key = (source.topic, source.profile.name)
            if key in self._sync_sources:
                raise ValueError("duplicate synchronization source")
            self._sync_sources[key] = source
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
        self.served_sync: asyncio.Queue[SyncServeResult] = asyncio.Queue(
            queue_size)
        self.connected = asyncio.Event()
        self.last_error: BaseException | None = None
        self._socket: SecureWebSocket | None = None
        self._stop = asyncio.Event()
        self._commit_lock = asyncio.Lock()
        self._committed: dict[Stream, int] = {}
        self._restored = False
        self._running = False
        self._sync_receivers: dict[
            bytes, tuple[SyncReceiver, asyncio.Future[SyncPullResult]]
        ] = {}
        self._sync_senders: dict[bytes, SyncSender] = {}
        self._sync_sender_timeouts: dict[bytes, asyncio.Task[None]] = {}
        self._content_waiters: dict[
            bytes, asyncio.Future[ContentResult]
        ] = {}
        self._content_service_tasks: set[asyncio.Task[None]] = set()
        self._content_failure: asyncio.Future[None] | None = None

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
                          frame: Mapping[str, object], *,
                          topic: str | None = None,
                          profile: str | None = None) -> None:
        frame_type = cast(str, frame["type"])
        if frame_type in {"gossip", "interests", "have", "want"}:
            operation = _operation(frame)
        elif frame_type in {"content/find", "content/found"}:
            operation = "content/find"
        elif frame_type in {
                "content/want", "content/block", "content/dont-have"}:
            operation = "content/read"
        else:
            operation = "synchronize"
        context: dict[str, object] = {
            "op": operation,
            "direction": direction,
            "peer_authority": self.expected_authority,
            "frame_type": frame_type,
            "frame": frame,
        }
        if frame["type"] == "gossip":
            context.update({
                "origin_authority": frame["origin"],
                "topic": frame["topic"],
                "payload": frame["payload"],
            })
        if frame_type.startswith("content/"):
            context["cid"] = frame["cid"]
        if topic is not None:
            context["topic"] = topic
        if profile is not None:
            context["profile"] = profile
        try:
            decision = self.authorize(context)
        except Exception as error:
            raise DisseminationClientError(
                "client authorization failed") from error
        if inspect.isawaitable(decision):
            try:
                decision = await asyncio.wait_for(
                    cast(Awaitable[bool], decision),
                    timeout=self.authorization_timeout,
                )
            except asyncio.TimeoutError as error:
                raise DisseminationClientError(
                    "client authorization timed out") from error
            except Exception as error:
                raise DisseminationClientError(
                    "client authorization failed") from error
        if decision is not True:
            raise DisseminationClientError("client authorization denied")

    async def _send(self, socket: SecureWebSocket,
                    frame: Mapping[str, object]) -> None:
        frame = validate_frame(frame)
        await self._authorized("outbound", frame)
        await socket.send_frame(encode_extension_frame(
            DISSEMINATION_CAPABILITY,
            frame,
            selected_capabilities=socket.peer.capabilities,
        ))

    async def _send_sync(self, socket: SecureWebSocket,
                         frame: Mapping[str, object], *, topic: str,
                         profile: str) -> None:
        await self._authorized("outbound", frame, topic=topic, profile=profile)
        await socket.send_frame(encode_extension_frame(
            SYNC_CAPABILITY,
            dict(frame),
            selected_capabilities=socket.peer.capabilities,
        ))

    async def _send_content(self, socket: SecureWebSocket,
                            frame: Mapping[str, object]) -> None:
        checked = validate_content_frame(frame)
        await self._authorized("outbound", checked)
        await socket.send_frame(encode_extension_frame(
            CONTENT_CAPABILITY,
            checked,
            selected_capabilities=socket.peer.capabilities,
        ))

    @staticmethod
    def _content_now_ms() -> int:
        return int(time.monotonic() * 1000)

    async def _receive_content(self, socket: SecureWebSocket,
                               frame: object) -> None:
        checked = validate_content_frame(frame)
        frame_type = cast(str, checked["type"])
        await self._authorized("inbound", checked)
        if frame_type in {"content/find", "content/want"}:
            if self.content_service is None:
                raise DisseminationClientError(
                    "peer requested content from a client without a service")
            if (len(self._content_service_tasks)
                    >= self.max_content_service_requests):
                raise DisseminationClientError(
                    "too many active content service requests")
            task = asyncio.create_task(self._serve_content(socket, checked))
            self._content_service_tasks.add(task)
            task.add_done_callback(self._content_service_tasks.discard)
            return
        result = self.content.receive(
            self.expected_authority, checked, now_ms=self._content_now_ms())
        if result is None:
            return
        completed = self._content_waiters.pop(result.request, None)
        if completed is not None and not completed.done():
            completed.set_result(result)

    async def _serve_content(self, socket: SecureWebSocket,
                             frame: Mapping[str, object]) -> None:
        try:
            if self.content_service is None:
                raise DisseminationClientError("content service disappeared")
            # Client policy already gated the carrier. The service applies its
            # separate authorization-before-storage policy to the same peer.
            response = await self.content_service.receive(
                self.expected_authority, frame)
            await self._send_content(socket, response)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            failure = self._content_failure
            if failure is not None and not failure.done():
                wrapped = DisseminationClientError(
                    "content service request failed")
                wrapped.__cause__ = error
                failure.set_exception(wrapped)
            await socket.websocket.close()

    def _remove_sync_sender(self, session: bytes) -> None:
        self._sync_senders.pop(session, None)
        timeout = self._sync_sender_timeouts.pop(session, None)
        if timeout is not None and timeout is not asyncio.current_task():
            timeout.cancel()

    def _record_sync_served(self, result: SyncServeResult) -> None:
        try:
            self.served_sync.put_nowait(result)
        except asyncio.QueueFull as error:
            raise DisseminationClientError(
                "synchronization completion queue is full") from error

    async def _expire_sync_sender(self, socket: SecureWebSocket,
                                  session: bytes, topic: str,
                                  profile: str) -> None:
        try:
            await asyncio.sleep(self.sync_timeout)
            sender = self._sync_senders.get(session)
            if sender is None:
                return
            self._remove_sync_sender(session)
            self._record_sync_served(SyncServeResult(
                session, topic, profile, "timed-out"))
            await self._send_sync(
                socket,
                {"v": 1, "type": "sync/abort", "session": session,
                 "code": "timeout"},
                topic=topic,
                profile=profile,
            )
        except asyncio.CancelledError:
            return
        except BaseException:
            await socket.websocket.close()

    @staticmethod
    def _sync_open_scope(frame: Mapping[str, object]) -> tuple[bytes, str, str]:
        if (set(frame) != {"v", "type", "session", "topic", "profile",
                           "summary"}
                or frame.get("v") != 1 or frame.get("type") != "sync/open"
                or not isinstance(frame.get("session"), bytes)
                or len(cast(bytes, frame["session"])) != 16
                or not isinstance(frame.get("topic"), str)
                or not cast(str, frame["topic"])
                or not isinstance(frame.get("profile"), str)
                or not cast(str, frame["profile"])
                or not isinstance(frame.get("summary"), bytes)):
            raise DisseminationClientError("invalid synchronization open frame")
        return (cast(bytes, frame["session"]), cast(str, frame["topic"]),
                cast(str, frame["profile"]))

    async def _open_sync_sender(self, socket: SecureWebSocket,
                                frame: Mapping[str, object]) -> None:
        session, topic, profile_name = self._sync_open_scope(frame)
        if (session in self._sync_receivers or session in self._sync_senders
                or len(self._sync_receivers) + len(self._sync_senders)
                >= self.max_sync_sessions):
            raise DisseminationClientError("sync session limit or collision")
        # Policy runs before source lookup or snapshot IO so an unauthorized
        # peer cannot probe whether an application topic exists.
        await self._authorized(
            "inbound", frame, topic=topic, profile=profile_name)
        source = self._sync_sources.get((topic, profile_name))
        if source is None:
            raise DisseminationClientError("synchronization open was not served")
        context: dict[str, object] = {
            "op": "synchronize",
            "direction": "inbound",
            "peer_authority": self.expected_authority,
            "topic": topic,
            "profile": profile_name,
            "frame": dict(frame),
        }
        try:
            snapshot = source.snapshot(context)
            if inspect.isawaitable(snapshot):
                snapshot = await asyncio.wait_for(
                    cast(Awaitable[SyncSnapshot], snapshot), self.sync_timeout)
            if not isinstance(snapshot, SyncSnapshot):
                raise TypeError("snapshot callback returned the wrong type")
            sender = SyncSender.from_open(
                dict(frame), source.profile, snapshot.value,
                snapshot.frontiers)
        except Exception as error:
            raise DisseminationClientError(
                "synchronization source failed") from error
        self._sync_senders[session] = sender
        pending = sender.start()
        if sender.status == "synchronizing":
            self._sync_sender_timeouts[session] = asyncio.create_task(
                self._expire_sync_sender(
                    socket, session, topic, profile_name))
        for reply in pending:
            await self._send_sync(
                socket, reply, topic=topic, profile=profile_name)
        if sender.status != "synchronizing":
            self._remove_sync_sender(session)
            self._record_sync_served(SyncServeResult(
                session, topic, profile_name, sender.status))

    async def _receive_sync_sender(self, socket: SecureWebSocket,
                                   sender: SyncSender,
                                   frame: Mapping[str, object]) -> None:
        await self._authorized(
            "inbound", frame, topic=sender.topic,
            profile=sender.profile.name)
        try:
            pending = sender.receive(dict(frame))
        except Exception as error:
            raise DisseminationClientError(
                "synchronization sender rejected a frame") from error
        for reply in pending:
            await self._send_sync(
                socket, reply, topic=sender.topic,
                profile=sender.profile.name)
        if sender.status != "synchronizing":
            self._remove_sync_sender(sender.session)
            self._record_sync_served(SyncServeResult(
                sender.session, sender.topic, sender.profile.name,
                sender.status))

    async def _receive_sync(self, socket: SecureWebSocket,
                            frame: object) -> None:
        if not isinstance(frame, dict):
            raise DisseminationClientError("invalid synchronization frame")
        if frame.get("type") == "sync/open":
            await self._open_sync_sender(socket, frame)
            return
        session = frame.get("session")
        active = self._sync_receivers.get(session) \
            if isinstance(session, bytes) else None
        serving = self._sync_senders.get(session) \
            if isinstance(session, bytes) else None
        if serving is not None:
            await self._receive_sync_sender(socket, serving, frame)
            return
        if active is None:
            raise DisseminationClientError("frame is for an unknown sync session")
        receiver, completed = active
        await self._authorized(
            "inbound", frame, topic=receiver.topic, profile=receiver.profile.name)
        try:
            reply = receiver.receive(frame)
        except Exception as error:
            raise DisseminationClientError(
                "synchronization profile rejected a frame") from error
        if reply is not None:
            await self._send_sync(
                socket, reply, topic=receiver.topic,
                profile=receiver.profile.name)
        if receiver.status == "desynchronized":
            self._sync_receivers.pop(receiver.session, None)
            if not completed.done():
                completed.set_exception(DisseminationClientError(
                    f"peer aborted synchronization: {frame.get('code')}"))
            return
        if receiver.status == "synchronized":
            await self.commit_frontiers(receiver.frontiers)
            self._sync_receivers.pop(receiver.session, None)
            if not completed.done():
                completed.set_result(SyncPullResult(
                    receiver.value, dict(receiver.frontiers)))

    async def _receive(self, socket: SecureWebSocket) -> None:
        while not self._stop.is_set():
            encoded = await socket.recv_frame()
            try:
                capability, value = decode_extension_frame(
                    encoded, selected_capabilities=socket.peer.capabilities)
                if capability == SYNC_CAPABILITY:
                    await self._receive_sync(socket, value)
                    continue
                if capability == CONTENT_CAPABILITY:
                    await self._receive_content(socket, value)
                    continue
                if capability != DISSEMINATION_CAPABILITY:
                    raise DisseminationClientError("unexpected client extension")
                frame = validate_frame(value)
                # Authenticate a claimed origin before policy is allowed to
                # rely on it. The session verifies again at its pure boundary.
                if frame["type"] == "gossip":
                    verify_publication(frame)
            except DisseminationClientError:
                raise
            except Exception as error:
                raise DisseminationClientError(
                    "invalid portable client frame") from error
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
        self._content_failure = asyncio.get_running_loop().create_future()
        receive: asyncio.Task[None] | None = None
        try:
            selected = set(socket.peer.capabilities)
            portable_dissemination = \
                set(DISSEMINATION_CAPABILITIES).issubset(selected)
            if self.require_dissemination and not portable_dissemination:
                raise DisseminationClientError(
                    "peer did not negotiate portable dissemination")
            if (not portable_dissemination
                    and SYNC_CAPABILITY not in selected
                    and CONTENT_CAPABILITY not in selected):
                raise DisseminationClientError(
                    "peer negotiated no supported application capability")
            if portable_dissemination:
                for frame in self.session.connected():
                    await self._send(socket, frame)
            self.connected.set()
            self.last_error = None
            receive = asyncio.create_task(self._receive(socket))
            while not self._stop.is_set():
                done, _pending = await asyncio.wait(
                    (receive, self._content_failure),
                    timeout=self.have_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._content_failure in done:
                    await self._content_failure
                if receive in done:
                    await receive
                    break
                if portable_dissemination:
                    await self._send(socket, self.session.have())
        finally:
            if receive is not None:
                if not receive.done():
                    receive.cancel()
                try:
                    await receive
                except BaseException:
                    pass
            content_tasks = tuple(self._content_service_tasks)
            for task in content_tasks:
                task.cancel()
            if content_tasks:
                await asyncio.gather(*content_tasks, return_exceptions=True)
            self._content_service_tasks.clear()
            if (self._content_failure is not None
                    and not self._content_failure.done()):
                self._content_failure.cancel()
            self._content_failure = None
            self.connected.clear()
            self.session.disconnected()
            self._socket = None
            for _receiver, completed in self._sync_receivers.values():
                if not completed.done():
                    completed.set_exception(DisseminationClientError(
                        "connection closed during synchronization"))
            self._sync_receivers.clear()
            for session in tuple(self._sync_senders):
                self._remove_sync_sender(session)
            for result in self.content.disconnected(self.expected_authority):
                completed = self._content_waiters.pop(result.request, None)
                if completed is not None and not completed.done():
                    completed.set_result(result)
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

    async def recv_sync_served(self) -> SyncServeResult:
        """Wait for one incoming sync session to finish, abort, or time out."""
        return await self.served_sync.get()

    async def synchronize(self, topic: str, profile: SyncProfile,
                          value: Any, *, session: bytes | None = None,
                          timeout: float | None = None) -> SyncPullResult:
        """Pull and atomically commit one state sync on the live peer socket."""
        socket = self._socket
        if socket is None or not self.connected.is_set():
            raise DisseminationClientError("synchronization requires a connection")
        if SYNC_CAPABILITY not in set(socket.peer.capabilities):
            raise DisseminationClientError("peer did not negotiate synchronization")
        if (len(self._sync_receivers) + len(self._sync_senders)
                >= self.max_sync_sessions):
            raise DisseminationClientError("too many active sync sessions")
        deadline = self.sync_timeout if timeout is None else timeout
        if not 0 < deadline <= 3600:
            raise ValueError("invalid synchronization timeout")
        session_id = secrets.token_bytes(16) if session is None else session
        receiver = SyncReceiver(session_id, topic, profile, value)
        if receiver.session in self._sync_receivers:
            raise DisseminationClientError("synchronization session already exists")
        completed: asyncio.Future[SyncPullResult] = \
            asyncio.get_running_loop().create_future()
        self._sync_receivers[receiver.session] = (receiver, completed)
        try:
            await self._send_sync(
                socket,
                open_frame(receiver.session, topic, profile, receiver.value),
                topic=topic,
                profile=profile.name,
            )
            return await asyncio.wait_for(asyncio.shield(completed), deadline)
        except asyncio.TimeoutError as error:
            try:
                await self._send_sync(
                    socket,
                    {"v": 1, "type": "sync/abort",
                     "session": receiver.session, "code": "timeout"},
                    topic=topic,
                    profile=profile.name,
                )
            except BaseException:
                pass
            raise DisseminationClientError("synchronization timed out") from error
        finally:
            self._sync_receivers.pop(receiver.session, None)
            if not completed.done():
                completed.cancel()

    async def _content_request(self, operation: str, cid: bytes, *,
                               timeout: float | None) -> ContentResult:
        socket = self._socket
        if socket is None or not self.connected.is_set():
            raise DisseminationClientError("content request requires a connection")
        if CONTENT_CAPABILITY not in set(socket.peer.capabilities):
            raise DisseminationClientError("peer did not negotiate content")
        max_deadline = self.content.timeout_ms / 1000
        deadline = max_deadline if timeout is None else timeout
        if not 0 < deadline <= max_deadline:
            raise ValueError("invalid content request timeout")
        now_ms = self._content_now_ms()
        try:
            frame = (self.content.want if operation == "content/want"
                     else self.content.find)(
                         self.expected_authority, cid, now_ms=now_ms)
        except Exception as error:
            raise DisseminationClientError(
                "content request could not be opened") from error
        request = cast(bytes, frame["request"])
        completed: asyncio.Future[ContentResult] = \
            asyncio.get_running_loop().create_future()
        self._content_waiters[request] = completed
        try:
            await self._send_content(socket, frame)
            return await asyncio.wait_for(asyncio.shield(completed), deadline)
        except asyncio.TimeoutError as error:
            self.content.pending.pop(request, None)
            raise DisseminationClientError("content request timed out") from error
        except BaseException:
            self.content.pending.pop(request, None)
            raise
        finally:
            self._content_waiters.pop(request, None)
            if not completed.done():
                completed.cancel()

    async def fetch_content(self, cid: bytes, *,
                            timeout: float | None = None) -> ContentResult:
        """Fetch one CID-verified block from the live authenticated peer."""
        return await self._content_request(
            "content/want", cid, timeout=timeout)

    async def find_content(self, cid: bytes, *,
                           timeout: float | None = None) -> ContentResult:
        """Ask the live authenticated peer for bounded provider hints."""
        return await self._content_request(
            "content/find", cid, timeout=timeout)

    async def close(self) -> None:
        self._stop.set()
        if self._socket is not None:
            await self._socket.websocket.close()


__all__ = [
    "Authorization", "CLIENT_CAPABILITIES", "DISSEMINATION_CAPABILITIES",
    "Delivery", "DisseminationClient", "DisseminationClientError",
    "SnapshotProvider", "SyncPullResult", "SyncServeResult", "SyncSnapshot",
    "SyncSource", "dissemination_identity",
]
