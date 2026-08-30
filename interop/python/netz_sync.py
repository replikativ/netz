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


class SyncError(ValueError):
    """A malformed frame or rejected synchronization transition."""


class SyncProfile(Protocol):
    name: str

    def describe(self, value: Any) -> bytes: ...

    def begin(self, value: Any) -> Any: ...

    def apply_chunk(self, staging: Any, chunk: bytes) -> Any: ...

    def commit(self, value: Any, staging: Any, checkpoint: bytes) -> Any: ...


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

    def __post_init__(self) -> None:
        self.session = _session(self.session)
        if self.max_chunks <= 0:
            raise SyncError("max_chunks must be positive")
        self.staging = self.profile.begin(self.value)
        self.next_index = 0
        self.status = "synchronizing"

    def receive(self, frame: dict[str, Any]) -> dict[str, Any] | None:
        if self.status != "synchronizing":
            raise SyncError("synchronization session is no longer active")
        if frame.get("v") != 1 or frame.get("session") != self.session:
            raise SyncError("frame is for an unknown session or profile version")
        frame_type = frame.get("type")
        if frame_type == "sync/chunk":
            index = frame.get("index")
            data = frame.get("data")
            if not isinstance(index, int) or index < 0 or not isinstance(data, bytes):
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
            count = frame.get("count")
            checkpoint = frame.get("checkpoint")
            frontiers = frame.get("frontiers")
            if (count != self.next_index or frame.get("topic") != self.topic
                    or not isinstance(checkpoint, bytes) or not isinstance(frontiers, dict)):
                raise SyncError("sync/done does not cover the staged prefix")
            self.value = self.profile.commit(self.value, self.staging, checkpoint)
            self.status = "synchronized"
            return None
        if frame_type == "sync/abort":
            self.status = "desynchronized"
            return None
        raise SyncError("unexpected synchronization frame")


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

    def commit(self, value: set[str], staging: set[str], checkpoint: bytes) -> set[str]:
        described = loads(checkpoint)
        if not isinstance(described, list) or not set(described).issubset(staging):
            raise SyncError("join-set checkpoint was not fully staged")
        return set(value) | staging


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
               checkpoint: bytes) -> dict[str, Any]:
        expected = self._decode(checkpoint)
        if expected != staging:
            raise SyncError("root checkpoint does not match staged value")
        if value is None or (staging["term"], staging["index"]) > (value["term"], value["index"]):
            return staging
        return value
