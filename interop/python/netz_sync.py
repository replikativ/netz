"""Pure Netz draft-00 synchronization state machines and reference profiles.

This module is transport-independent: a carrier exchanges decoded CBOR frames
between :class:`SyncSender` and :class:`SyncReceiver`. The receiver keeps
application state isolated until ``sync/done`` so interrupted sessions cannot
expose partial state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from netz_codec import dumps, loads

JOIN_PROFILE = "netz/join-semilattice/1"
ROOT_PROFILE = "netz/root-log/1"
MAX_SAFE_INTEGER = (1 << 53) - 1


class SyncError(ValueError):
    """A malformed frame or rejected synchronization transition."""


@dataclass(frozen=True)
class CommitReceipt:
    """State and frontiers minted by one successful local profile commit."""

    value: Any
    frontiers: list[dict[str, Any]]
    frontier_bytes: int


@dataclass(frozen=True)
class SyncResponse:
    """Opaque chunks and checkpoint produced for one remote summary."""

    chunks: tuple[bytes, ...]
    checkpoint: bytes


class SyncProfile(Protocol):
    name: str

    def describe(self, value: Any) -> bytes: ...

    def respond(self, value: Any, remote_summary: bytes) -> SyncResponse: ...

    def begin(self, value: Any) -> Any: ...

    def apply_chunk(self, staging: Any, chunk: bytes) -> Any: ...

    def commit(self, value: Any, staging: Any, checkpoint: bytes,
               claimed_frontiers: list[dict[str, Any]]) -> CommitReceipt: ...


def _session(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != 16:
        raise SyncError("session must be exactly 16 bytes")
    return value


def _topic(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise SyncError("topic must be non-empty text")
    return value


def _valid_frontiers(topic: str, frontiers: list[dict[str, Any]],
                     max_streams: int, max_bytes: int) -> bool:
    if len(frontiers) > max_streams:
        return False
    previous: tuple[bytes, int, bytes] | None = None
    for item in frontiers:
        if (not isinstance(item, dict)
                or set(item) != {"origin", "epoch", "topic", "through"}
                or not isinstance(item["origin"], bytes)
                or len(item["origin"]) != 32
                or type(item["epoch"]) is not int
                or not 0 <= item["epoch"] <= MAX_SAFE_INTEGER
                or item["topic"] != topic
                or type(item["through"]) is not int
                or not 0 <= item["through"] <= MAX_SAFE_INTEGER):
            return False
        order = (item["origin"], item["epoch"], item["topic"].encode())
        if previous is not None and order <= previous:
            return False
        previous = order
    return len(dumps(frontiers)) <= max_bytes


def open_frame(session: bytes, topic: str, profile: SyncProfile, value: Any) -> dict[str, Any]:
    """Build a portable ``sync/open`` frame from an application value."""
    return {
        "v": 1,
        "type": "sync/open",
        "session": _session(session),
        "topic": _topic(topic),
        "profile": profile.name,
        "summary": profile.describe(value),
    }


@dataclass
class SyncSender:
    """One bounded sender using cumulative ACKs and a chunk window."""

    session: bytes
    topic: str
    profile: SyncProfile
    value: Any
    remote_summary: bytes
    frontiers: list[dict[str, Any]]
    window: int = 4
    max_chunks: int = 4096
    max_frontier_streams: int = 1024
    max_frontier_bytes: int = 256 * 1024

    def __post_init__(self) -> None:
        self.session = _session(self.session)
        self.topic = _topic(self.topic)
        if not isinstance(self.remote_summary, bytes):
            raise SyncError("summary must be opaque bytes")
        if (type(self.window) is not int or self.window <= 0
                or type(self.max_chunks) is not int or self.max_chunks <= 0
                or type(self.max_frontier_streams) is not int
                or self.max_frontier_streams < 0
                or type(self.max_frontier_bytes) is not int
                or self.max_frontier_bytes <= 0):
            raise SyncError("sender limits must be bounded positive integers")
        if (not isinstance(self.frontiers, list)
                or not _valid_frontiers(
                    self.topic, self.frontiers, self.max_frontier_streams,
                    self.max_frontier_bytes)):
            raise SyncError("sender has invalid committed frontiers")
        # Own an immutable-by-convention snapshot rather than retaining a
        # caller-owned list that could change after validation.
        self.frontiers = loads(dumps(self.frontiers))
        response = self.profile.respond(self.value, self.remote_summary)
        if (not isinstance(response, SyncResponse)
                or not isinstance(response.chunks, tuple)
                or len(response.chunks) > self.max_chunks
                or not all(isinstance(chunk, bytes) for chunk in response.chunks)
                or not isinstance(response.checkpoint, bytes)):
            raise SyncError("profile returned an invalid sync response")
        self.chunks = response.chunks
        self.checkpoint = response.checkpoint
        self.next_to_send = 0
        self.acked = -1
        self.status = "ready"

    @classmethod
    def from_open(cls, frame: dict[str, Any], profile: SyncProfile, value: Any,
                  frontiers: list[dict[str, Any]], **limits: Any) -> "SyncSender":
        """Validate an exact ``sync/open`` frame and prepare its response."""
        if (not isinstance(frame, dict)
                or set(frame) != {"v", "type", "session", "topic", "profile",
                                  "summary"}
                or frame.get("v") != 1 or frame.get("type") != "sync/open"
                or frame.get("profile") != profile.name
                or not isinstance(frame.get("summary"), bytes)):
            raise SyncError("invalid sync/open frame")
        return cls(frame["session"], frame["topic"], profile, value,
                   frame["summary"], frontiers, **limits)

    def start(self) -> list[dict[str, Any]]:
        """Emit the initial window, or ``sync/done`` for an empty response."""
        if self.status != "ready":
            raise SyncError("synchronization sender was already started")
        self.status = "synchronizing"
        return self._advance()

    def receive(self, frame: dict[str, Any]) -> list[dict[str, Any]]:
        """Consume one exact cumulative ACK or remote abort."""
        if self.status != "synchronizing":
            raise SyncError("synchronization sender is no longer active")
        if (not isinstance(frame, dict) or frame.get("v") != 1
                or frame.get("session") != self.session):
            raise SyncError("frame is for an unknown session or profile version")
        if frame.get("type") == "sync/ack":
            if set(frame) != {"v", "type", "session", "through"}:
                raise SyncError("invalid sync/ack fields")
            through = frame.get("through")
            if (type(through) is not int or not 0 <= through <= MAX_SAFE_INTEGER
                    or through >= self.next_to_send):
                raise SyncError("acknowledgement covers an unsent chunk")
            self.acked = max(self.acked, through)
            return self._advance()
        if frame.get("type") == "sync/abort":
            if (set(frame) != {"v", "type", "session", "code"}
                    or not isinstance(frame.get("code"), str)
                    or not frame["code"]):
                raise SyncError("invalid sync/abort fields")
            self.status = "desynchronized"
            return []
        raise SyncError("unexpected synchronization frame")

    def _advance(self) -> list[dict[str, Any]]:
        through = min(len(self.chunks), self.acked + 1 + self.window)
        frames = [
            {"v": 1, "type": "sync/chunk", "session": self.session,
             "index": index, "data": self.chunks[index]}
            for index in range(self.next_to_send, through)
        ]
        self.next_to_send = through
        if (self.acked == len(self.chunks) - 1
                and self.next_to_send == len(self.chunks)):
            frames.append({
                "v": 1, "type": "sync/done", "session": self.session,
                "count": len(self.chunks), "topic": self.topic,
                "checkpoint": self.checkpoint, "frontiers": self.frontiers,
            })
            self.status = "synchronized"
        return frames


@dataclass
class SyncReceiver:
    """One bounded, ordered pull session with atomic application commit."""

    session: bytes
    topic: str
    profile: SyncProfile
    value: Any
    max_chunks: int = 4096
    max_frontier_streams: int = 1024
    max_frontier_bytes: int = 256 * 1024

    def __post_init__(self) -> None:
        self.session = _session(self.session)
        self.topic = _topic(self.topic)
        if (type(self.max_chunks) is not int or self.max_chunks <= 0
                or type(self.max_frontier_streams) is not int
                or self.max_frontier_streams < 0
                or type(self.max_frontier_bytes) is not int
                or self.max_frontier_bytes <= 0):
            raise SyncError("receiver limits must be bounded positive integers")
        self.staging = self.profile.begin(self.value)
        self.next_index = 0
        self.status = "synchronizing"
        self.frontiers: dict[tuple[bytes, int, str], int] = {}

    def receive(self, frame: dict[str, Any]) -> dict[str, Any] | None:
        if self.status != "synchronizing":
            raise SyncError("synchronization session is no longer active")
        if (not isinstance(frame, dict) or frame.get("v") != 1
                or frame.get("session") != self.session):
            raise SyncError("frame is for an unknown session or profile version")
        frame_type = frame.get("type")
        if frame_type == "sync/chunk":
            if set(frame) != {"v", "type", "session", "index", "data"}:
                raise SyncError("invalid sync/chunk fields")
            index = frame.get("index")
            data = frame.get("data")
            if (type(index) is not int or not 0 <= index <= MAX_SAFE_INTEGER
                    or not isinstance(data, bytes)):
                raise SyncError("invalid sync/chunk fields")
            if index < self.next_index:
                return {"v": 1, "type": "sync/ack", "session": self.session,
                        "through": self.next_index - 1}
            if index != self.next_index:
                raise SyncError("sync chunks must be applied in order")
            if self.next_index >= self.max_chunks:
                raise SyncError("maximum chunk count exceeded")
            self.staging = self.profile.apply_chunk(self.staging, data)
            self.next_index += 1
            return {"v": 1, "type": "sync/ack", "session": self.session,
                    "through": index}
        if frame_type == "sync/done":
            if set(frame) != {"v", "type", "session", "count", "topic",
                              "checkpoint", "frontiers"}:
                raise SyncError("invalid sync/done fields")
            count = frame.get("count")
            checkpoint = frame.get("checkpoint")
            frontiers = frame.get("frontiers")
            if (type(count) is not int or count != self.next_index
                    or not 0 <= count <= MAX_SAFE_INTEGER
                    or frame.get("topic") != self.topic
                    or not isinstance(checkpoint, bytes) or not isinstance(frontiers, list)):
                raise SyncError("sync/done does not cover the staged prefix")
            if not _valid_frontiers(
                    self.topic, frontiers, self.max_frontier_streams,
                    self.max_frontier_bytes):
                raise SyncError("sync/done contains invalid frontier claims")
            receipt = self.profile.commit(self.value, self.staging, checkpoint,
                                          frontiers)
            if (not isinstance(receipt, CommitReceipt)
                    or not isinstance(receipt.frontiers, list)
                    or len(receipt.frontiers) > self.max_frontier_streams
                    or type(receipt.frontier_bytes) is not int
                    or receipt.frontier_bytes < 0
                    or receipt.frontier_bytes > self.max_frontier_bytes
                    or receipt.frontier_bytes != len(dumps(receipt.frontiers))
                    or not _valid_frontiers(
                        self.topic, receipt.frontiers,
                        self.max_frontier_streams, self.max_frontier_bytes)):
                raise SyncError("profile returned invalid committed frontiers")
            self.value = receipt.value
            for item in receipt.frontiers:
                stream = (item["origin"], item["epoch"], item["topic"])
                self.frontiers[stream] = max(self.frontiers.get(stream, -1),
                                             item["through"])
            self.status = "synchronized"
            return None
        if frame_type == "sync/abort":
            if (set(frame) != {"v", "type", "session", "code"}
                    or not isinstance(frame.get("code"), str)
                    or not frame["code"]):
                raise SyncError("invalid sync/abort fields")
            self.status = "desynchronized"
            return None
        raise SyncError("unexpected synchronization frame")

class JoinSetProfile:
    """Portable reference join-semilattice over a set of CBOR text values."""

    name = JOIN_PROFILE

    def describe(self, value: set[str]) -> bytes:
        if not isinstance(value, set) or not all(isinstance(item, str) for item in value):
            raise SyncError("join-set state must be a set of text")
        return dumps(sorted(value))

    def respond(self, value: set[str], remote_summary: bytes) -> SyncResponse:
        remote = loads(remote_summary)
        if not isinstance(remote, list) or not all(isinstance(item, str) for item in remote):
            raise SyncError("join-set summary must be an array of text")
        local = set(value)
        missing = sorted(local - set(remote))
        chunks = () if not missing else (dumps(missing),)
        return SyncResponse(chunks, self.describe(local))

    def begin(self, value: set[str]) -> set[str]:
        return set(value)

    def apply_chunk(self, staging: set[str], chunk: bytes) -> set[str]:
        values = loads(chunk)
        if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
            raise SyncError("join-set chunk must be an array of text")
        return staging | set(values)

    def commit(self, value: set[str], staging: set[str], checkpoint: bytes,
               claimed_frontiers: list[dict[str, Any]]) -> CommitReceipt:
        described = loads(checkpoint)
        if not isinstance(described, list) or not set(described).issubset(staging):
            raise SyncError("join-set checkpoint was not fully staged")
        # This reference checkpoint contains state, not a causal proof. The
        # sender's frontier claim is therefore deliberately ignored.
        frontiers: list[dict[str, Any]] = []
        return CommitReceipt(set(value) | staging, frontiers,
                             len(dumps(frontiers)))


class RootLogProfile:
    """Portable single-writer, monotonically fenced root-log profile."""

    name = ROOT_PROFILE

    def __init__(self, writer: str):
        self.writer = writer

    def _decode(self, encoded: bytes) -> dict[str, Any]:
        record = loads(encoded)
        if (not isinstance(record, dict) or record.get("writer") != self.writer
                or type(record.get("term")) is not int or record["term"] < 0
                or type(record.get("index")) is not int or record["index"] < 0
                or "root" not in record):
            raise SyncError("invalid fenced root record")
        return record

    def _decode_optional(self, encoded: bytes) -> dict[str, Any] | None:
        value = loads(encoded)
        if value is None:
            return None
        return self._decode(encoded)

    def describe(self, value: dict[str, Any] | None) -> bytes:
        if value is not None:
            self._decode(dumps(value))
        return dumps(value)

    def respond(self, value: dict[str, Any] | None,
                remote_summary: bytes) -> SyncResponse:
        local = self._decode_optional(self.describe(value))
        remote = self._decode_optional(remote_summary)
        if local is None:
            winner = remote
        elif remote is None:
            winner = local
        else:
            local_position = (local["term"], local["index"])
            remote_position = (remote["term"], remote["index"])
            if local_position == remote_position and local["root"] != remote["root"]:
                raise SyncError("conflicting roots at one fence/index")
            winner = local if local_position > remote_position else remote
        chunks = ((dumps(local),) if local is not None and winner == local
                  and local != remote else ())
        return SyncResponse(chunks, dumps(winner))

    def begin(self, value: dict[str, Any] | None) -> dict[str, Any] | None:
        return value

    def apply_chunk(self, staging: dict[str, Any] | None, chunk: bytes) -> dict[str, Any]:
        incoming = self._decode(chunk)
        if staging is None:
            return incoming
        current_position = (staging["term"], staging["index"])
        incoming_position = (incoming["term"], incoming["index"])
        if incoming_position == current_position and incoming["root"] != staging["root"]:
            raise SyncError("conflicting roots at one fence/index")
        return incoming if incoming_position > current_position else staging

    def commit(self, value: dict[str, Any] | None,
               staging: dict[str, Any] | None,
               checkpoint: bytes,
               claimed_frontiers: list[dict[str, Any]]) -> CommitReceipt:
        expected = self._decode_optional(checkpoint)
        if expected != staging:
            raise SyncError("root checkpoint does not match staged value")
        # A production root envelope must sign its frontier alongside the
        # fence and content root before this profile may return it.
        frontiers: list[dict[str, Any]] = []
        return CommitReceipt(staging, frontiers, len(dumps(frontiers)))
