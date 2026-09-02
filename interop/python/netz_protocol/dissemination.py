"""Portable dissemination frames and bounded signed-publication receiving."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence, cast

from netz_codec import dumps, loads

from .publication import (
    MAX_SAFE_INTEGER,
    MAX_TOPIC_BYTES,
    parse_publication,
    sign_publication,
    verify_publication,
)

DISSEMINATION_CAPABILITY = "netz/dissemination/1"
MAX_INTERESTS = 1024
MAX_STREAMS = 1024
MAX_INTERVALS_PER_STREAM = 512
MAX_TOTAL_INTERVALS = 1024
MAX_WANT_RANGES = 16


class DisseminationError(ValueError):
    """A portable interests, repair, or publication frame is invalid."""


def _uint(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SAFE_INTEGER:
        raise DisseminationError(f"{name} is outside the portable uint range")
    return value


def _topic(value: object) -> str:
    if (type(value) is not str or not value
            or len(value.encode("utf-8")) > MAX_TOPIC_BYTES):
        raise DisseminationError("invalid portable topic")
    return value


def _authority(value: object) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise DisseminationError("origin must be a full 32-byte Authority ID")
    return value


def _strictly_ordered(values: Sequence[object], key) -> bool:
    keys = [key(value) for value in values]
    return all(left < right for left, right in zip(keys, keys[1:]))


def interests_frame(topics: Iterable[str], carries: Iterable[str] = (), *,
                    relay_all: bool = False) -> dict[str, object]:
    """Construct a canonical exact-topic interest advertisement."""
    if type(relay_all) is not bool:
        raise DisseminationError("relay_all must be boolean")
    topics = sorted({_topic(value) for value in topics}, key=str.encode)
    carries = sorted({_topic(value) for value in carries}, key=str.encode)
    if len(topics) > MAX_INTERESTS or len(carries) > MAX_INTERESTS:
        raise DisseminationError("too many interests")
    return {
        "v": 1, "type": "interests", "topics": topics,
        "carries": carries, "relay_all": relay_all,
    }


def _intervals(value: object) -> list[list[int]]:
    if not isinstance(value, list) or len(value) > MAX_INTERVALS_PER_STREAM:
        raise DisseminationError("invalid interval array")
    previous_hi = -2
    for interval in value:
        if (not isinstance(interval, list) or len(interval) != 2
                or _uint(interval[0], "interval lower bound") >
                _uint(interval[1], "interval upper bound")
                or interval[0] <= previous_hi + 1):
            raise DisseminationError("intervals are not canonical")
        previous_hi = interval[1]
    return value


def _stream_key(value: Mapping[str, object]) -> tuple[bytes, int, bytes]:
    return (
        _authority(value["origin"]),
        _uint(value["epoch"], "epoch"),
        _topic(value["topic"]).encode("utf-8"),
    )


def _stream(value: object) -> dict[str, object]:
    required = {"origin", "epoch", "topic", "ranges", "floor"}
    if not isinstance(value, dict) or set(value) != required:
        raise DisseminationError("invalid have stream schema")
    _authority(value["origin"])
    _uint(value["epoch"], "epoch")
    _topic(value["topic"])
    _intervals(value["ranges"])
    _uint(value["floor"], "repair floor")
    return value


def have_frame(streams: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Construct a canonical flattened anti-entropy summary."""
    checked = [_stream(dict(value)) for value in streams]
    if (len(checked) > MAX_STREAMS
            or sum(len(cast(list[object], value["ranges"]))
                   for value in checked)
            > MAX_TOTAL_INTERVALS):
        raise DisseminationError("too many have streams")
    checked.sort(key=_stream_key)
    if not _strictly_ordered(checked, _stream_key):
        raise DisseminationError("duplicate have stream")
    return {"v": 1, "type": "have", "streams": checked}


def _want_key(value: Mapping[str, object]) -> tuple[bytes, int, bytes, int, int]:
    return (
        *_stream_key(value),
        _uint(value["lo"], "lower bound"),
        _uint(value["hi"], "upper bound"),
    )


def _want(value: object) -> dict[str, object]:
    required = {"origin", "epoch", "topic", "lo", "hi"}
    if not isinstance(value, dict) or set(value) != required:
        raise DisseminationError("invalid want range schema")
    _authority(value["origin"])
    _uint(value["epoch"], "epoch")
    _topic(value["topic"])
    if _uint(value["lo"], "lower bound") > _uint(value["hi"], "upper bound"):
        raise DisseminationError("invalid want bounds")
    return value


def want_frame(ranges: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Construct one canonical bounded repair request."""
    checked = [_want(dict(value)) for value in ranges]
    if len(checked) > MAX_WANT_RANGES:
        raise DisseminationError("too many want ranges")
    checked.sort(key=_want_key)
    if not _strictly_ordered(checked, _want_key):
        raise DisseminationError("duplicate want range")
    return {"v": 1, "type": "want", "ranges": checked}


def validate_frame(value: object) -> dict[str, object]:
    """Validate one exact, canonical dissemination frame."""
    if not isinstance(value, dict) or value.get("v") != 1:
        raise DisseminationError("invalid dissemination frame")
    frame_type = value.get("type")
    if frame_type == "gossip":
        try:
            return parse_publication(value)
        except Exception as error:
            raise DisseminationError("invalid publication envelope") from error
    if frame_type == "interests":
        required = {"v", "type", "topics", "carries", "relay_all"}
        topics, carries = value.get("topics"), value.get("carries")
        if (set(value) != required or not isinstance(topics, list)
                or not isinstance(carries, list)
                or len(topics) > MAX_INTERESTS or len(carries) > MAX_INTERESTS
                or type(value.get("relay_all")) is not bool):
            raise DisseminationError("invalid interests frame")
        for topic in (*topics, *carries):
            _topic(topic)
        if (not _strictly_ordered(topics, str.encode)
                or not _strictly_ordered(carries, str.encode)):
            raise DisseminationError("interests are not canonical")
        return dict(value)
    if frame_type == "have":
        streams = value.get("streams")
        if (set(value) != {"v", "type", "streams"}
                or not isinstance(streams, list) or len(streams) > MAX_STREAMS):
            raise DisseminationError("invalid have frame")
        for stream in streams:
            _stream(stream)
        if (sum(len(cast(list[object], stream["ranges"]))
                for stream in streams)
                > MAX_TOTAL_INTERVALS
                or not _strictly_ordered(streams, _stream_key)):
            raise DisseminationError("have streams are not canonical")
        return dict(value)
    if frame_type == "want":
        ranges = value.get("ranges")
        if (set(value) != {"v", "type", "ranges"}
                or not isinstance(ranges, list)
                or len(ranges) > MAX_WANT_RANGES):
            raise DisseminationError("invalid want frame")
        for item in ranges:
            _want(item)
        if not _strictly_ordered(ranges, _want_key):
            raise DisseminationError("want ranges are not canonical")
        return dict(value)
    raise DisseminationError("unknown dissemination frame type")


def encode_frame(value: object) -> bytes:
    """Validate and deterministically encode one dissemination frame."""
    return dumps(validate_frame(value))


def decode_frame(encoded: bytes) -> dict[str, object]:
    """Decode and validate one deterministic dissemination frame."""
    try:
        return validate_frame(loads(encoded))
    except DisseminationError:
        raise
    except Exception as error:
        raise DisseminationError("invalid dissemination encoding") from error

Stream = tuple[bytes, int, str]


@dataclass(frozen=True)
class ReceiveResult:
    """Effects produced by accepting one publication."""

    delivered: tuple[dict[str, object], ...] = ()
    gaps: tuple[tuple[int, int], ...] = ()
    state_sync_required: bool = False
    duplicate: bool = False


class Publisher:
    """Topic-scoped sequence owner for one operational identity and epoch."""

    def __init__(self, genesis: dict[str, object], private_key: bytes, *,
                 epoch: int) -> None:
        self.genesis = genesis
        self.private_key = private_key
        self.epoch = epoch
        self._next: dict[str, int] = {}

    def publish(self, topic: str, payload: bytes) -> dict[str, object]:
        """Sign the next item in ``topic`` without coupling topic streams."""
        sequence = self._next.get(topic, 0)
        publication = sign_publication(
            self.genesis, self.private_key, epoch=self.epoch,
            sequence=sequence, topic=topic, payload=payload,
        )
        self._next[topic] = sequence + 1
        return publication


class OrderedReceiver:
    """Deduplicate and release contiguous prefixes per publication stream.

    WebSocket and Kabel preserve order on one connection. This state is still
    required because a P2P publication may arrive through multiple relays or
    after reconnect/repair. A buffer overflow is a state-sync request, never a
    reason to grow memory or silently apply a suffix over a missing prefix.
    """

    def __init__(self, *, max_pending_per_stream: int = 1024,
                 max_streams: int = 256) -> None:
        if max_pending_per_stream < 1 or max_streams < 1:
            raise ValueError("receiver bounds must be positive")
        self.max_pending_per_stream = max_pending_per_stream
        self.max_streams = max_streams
        self.next_sequence: dict[Stream, int] = {}
        self.pending: dict[Stream, dict[int, dict[str, object]]] = {}
        self.desynchronized: set[Stream] = set()

    @staticmethod
    def stream(publication: Mapping[str, object]) -> Stream:
        return (
            cast(bytes, publication["origin"]),
            cast(int, publication["epoch"]),
            cast(str, publication["topic"]),
        )

    def _release(self, stream: Stream) -> tuple[dict[str, object], ...]:
        delivered: list[dict[str, object]] = []
        waiting = self.pending.get(stream, {})
        sequence = self.next_sequence.get(stream, 0)
        while sequence in waiting:
            delivered.append(waiting.pop(sequence))
            sequence += 1
        self.next_sequence[stream] = sequence
        if not waiting:
            self.pending.pop(stream, None)
        return tuple(delivered)

    def receive(self, value: Mapping[str, object]) -> ReceiveResult:
        """Verify, deduplicate, buffer, and release one signed publication."""
        publication = verify_publication(value)
        stream = self.stream(publication)
        sequence = cast(int, publication["seq"])
        next_sequence = self.next_sequence.get(stream, 0)
        waiting = self.pending.get(stream)
        if sequence < next_sequence or (waiting is not None and sequence in waiting):
            return ReceiveResult(duplicate=True)
        if stream not in self.next_sequence and len(self.next_sequence) >= self.max_streams:
            # The caller must decide which application state to retain. An
            # attacker must not select an eviction victim through a new key.
            return ReceiveResult(state_sync_required=True)
        waiting = self.pending.setdefault(stream, {})
        waiting[sequence] = publication
        overflow = len(waiting) > self.max_pending_per_stream
        if overflow:
            # Keep the earliest suffix: it is the part most likely to become
            # releasable if recent-message repair closes the first gap.
            for item in sorted(waiting)[self.max_pending_per_stream:]:
                del waiting[item]
            self.desynchronized.add(stream)
        delivered = self._release(stream)
        next_sequence = self.next_sequence.get(stream, 0)
        waiting = self.pending.get(stream, {})
        gaps: tuple[tuple[int, int], ...] = ()
        if waiting:
            first = min(waiting)
            if first > next_sequence:
                gaps = ((next_sequence, first - 1),)
        return ReceiveResult(
            delivered=delivered,
            gaps=gaps,
            state_sync_required=overflow,
        )

    def commit_frontiers(self, frontiers: Mapping[Stream, int]) -> ReceiveResult:
        """Advance only after an application profile atomically commits state."""
        delivered: list[dict[str, object]] = []
        for stream, through in frontiers.items():
            if type(through) is not int or through < -1:
                raise ValueError("invalid committed publication frontier")
            waiting = self.pending.get(stream, {})
            self.pending[stream] = {
                sequence: value for sequence, value in waiting.items()
                if sequence > through
            }
            self.next_sequence[stream] = through + 1
            self.desynchronized.discard(stream)
            delivered.extend(self._release(stream))
        return ReceiveResult(delivered=tuple(delivered))


__all__ = [
    "DISSEMINATION_CAPABILITY", "DisseminationError", "OrderedReceiver",
    "Publisher", "ReceiveResult", "Stream", "decode_frame", "encode_frame",
    "have_frame", "interests_frame", "validate_frame", "want_frame",
]
