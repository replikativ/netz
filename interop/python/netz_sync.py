"""Pure Netz draft-00 synchronization receiver and reference profiles.

This module is transport-independent: a WebSocket client feeds decoded CBOR
frames into :class:`SyncReceiver` and sends the returned acknowledgement. It
keeps application state isolated until ``sync/done`` so interrupted sessions
cannot expose partial state.
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


class SyncProfile(Protocol):
    name: str

    def describe(self, value: Any) -> bytes: ...

    def begin(self, value: Any) -> Any: ...

    def apply_chunk(self, staging: Any, chunk: bytes) -> Any: ...

    def commit(self, value: Any, staging: Any, checkpoint: bytes,
               claimed_frontiers: list[dict[str, Any]]) -> CommitReceipt: ...


def _session(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != 16:
        raise SyncError("session must be exactly 16 bytes")
    return value


def open_frame(session: bytes, topic: str, profile: SyncProfile, value: Any) -> dict[str, Any]:
    """Build a portable ``sync/open`` frame from an application value."""
    if not isinstance(topic, str) or not topic:
        raise SyncError("topic must be non-empty text")
    return {
        "v": 1,
        "type": "sync/open",
        "session": _session(session),
        "topic": topic,
        "profile": profile.name,
        "summary": profile.describe(value),
    }


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
        if not isinstance(self.topic, str) or not self.topic:
            raise SyncError("topic must be non-empty text")
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
        if frame.get("v") != 1 or frame.get("session") != self.session:
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
            if (len(frontiers) > self.max_frontier_streams
                    or not self._valid_frontiers(frontiers)
                    or len(dumps(frontiers)) > self.max_frontier_bytes):
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
                    or not self._valid_frontiers(receipt.frontiers)):
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

    def _valid_frontiers(self, frontiers: list[dict[str, Any]]) -> bool:
        previous: tuple[bytes, int, bytes] | None = None
        for item in frontiers:
            if (not isinstance(item, dict)
                    or set(item) != {"origin", "epoch", "topic", "through"}
                    or not isinstance(item["origin"], bytes)
                    or len(item["origin"]) != 32
                    or type(item["epoch"]) is not int
                    or not 0 <= item["epoch"] <= MAX_SAFE_INTEGER
                    or item["topic"] != self.topic
                    or type(item["through"]) is not int
                    or not 0 <= item["through"] <= MAX_SAFE_INTEGER):
                return False
            order = (item["origin"], item["epoch"], item["topic"].encode())
            if previous is not None and order <= previous:
                return False
            previous = order
        return True


class JoinSetProfile:
    """Portable reference join-semilattice over a set of CBOR text values."""

    name = JOIN_PROFILE

    def describe(self, value: set[str]) -> bytes:
        return dumps(sorted(value))

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

    def describe(self, value: dict[str, Any] | None) -> bytes:
        return dumps(value)

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

    def commit(self, value: dict[str, Any] | None, staging: dict[str, Any],
               checkpoint: bytes,
               claimed_frontiers: list[dict[str, Any]]) -> CommitReceipt:
        expected = self._decode(checkpoint)
        if expected != staging:
            raise SyncError("root checkpoint does not match staged value")
        committed = value
        if value is None or (staging["term"], staging["index"]) > (value["term"], value["index"]):
            committed = staging
        # A production root envelope must sign its frontier alongside the
        # fence and content root before this profile may return it.
        frontiers: list[dict[str, Any]] = []
        return CommitReceipt(committed, frontiers, len(dumps(frontiers)))
