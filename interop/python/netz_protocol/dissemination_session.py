"""Transport-independent subscription, publication, and repair orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, cast

from .dissemination import (
    MAX_INTERVALS_PER_STREAM,
    MAX_STREAMS,
    MAX_TOTAL_INTERVALS,
    MAX_WANT_RANGES,
    OrderedReceiver,
    Publisher,
    ReceiveResult,
    Stream,
    have_frame,
    interests_frame,
    validate_frame,
    want_frame,
)

MAX_WANT_SPAN = 1024


@dataclass(frozen=True)
class SessionResult:
    """Application and network effects produced by one input frame."""

    outbound: tuple[dict[str, object], ...] = ()
    delivered: tuple[dict[str, object], ...] = ()
    state_sync_required: tuple[tuple[Stream, int, int], ...] = ()
    duplicate: bool = False


def _intervals(sequences: Iterable[int]) -> list[list[int]]:
    ordered = sorted(set(sequences))
    result: list[list[int]] = []
    for sequence in ordered:
        if result and sequence == result[-1][1] + 1:
            result[-1][1] = sequence
        else:
            result.append([sequence, sequence])
    return result


class DisseminationSession:
    """One bounded dissemination state machine, independent of a carrier.

    The session retains recent verified publications so it can answer repair
    requests. It intentionally does not persist state: applications that need
    crash-safe frontiers should persist delivered values before retaining and
    reusing this object across reconnects, or recover through state sync.
    """

    def __init__(self, *, topics: Iterable[str], publisher: Publisher | None = None,
                 receiver: OrderedReceiver | None = None,
                 carries: Iterable[str] = (), relay_all: bool = False,
                 history_size: int = 256, max_want_span: int = MAX_WANT_SPAN,
                 max_want_response: int = 32) -> None:
        if history_size < 1 or max_want_span < 1 or max_want_response < 1:
            raise ValueError("dissemination session bounds must be positive")
        self.topics = frozenset(topics)
        self.carries = frozenset(carries)
        self.relay_all = relay_all
        # Construction validates and canonicalizes all topic inputs.
        interests_frame(self.topics, self.carries, relay_all=relay_all)
        self.publisher = publisher
        self.receiver = receiver or OrderedReceiver()
        self.history_size = history_size
        self.max_want_span = max_want_span
        self.max_want_response = max_want_response
        self.remote_topics: frozenset[str] | None = None
        self.remote_carries: frozenset[str] = frozenset()
        self.remote_relay_all = False
        self._history: dict[tuple[Stream, int], dict[str, object]] = {}
        self._history_order: list[tuple[Stream, int]] = []

    def _remember(self, publication: Mapping[str, object]) -> None:
        stream = self.receiver.stream(publication)
        sequence = cast(int, publication["seq"])
        key = (stream, sequence)
        if key in self._history:
            return
        self._history[key] = dict(publication)
        self._history_order.append(key)
        while len(self._history_order) > self.history_size:
            expired = self._history_order.pop(0)
            self._history.pop(expired, None)

    def _known_intervals(self, stream: Stream) -> list[list[int]]:
        through = self.receiver.next_sequence.get(stream, 0)
        known = list(self.receiver.pending.get(stream, {}))
        known.extend(sequence for (candidate, sequence) in self._history
                     if candidate == stream and sequence >= through)
        suffix = _intervals(known)
        if through == 0:
            return suffix
        prefix = [0, through - 1]
        if suffix and suffix[0][0] == through:
            prefix[1] = suffix.pop(0)[1]
        return [prefix, *suffix]

    def have(self) -> dict[str, object]:
        """Describe bounded recent publication state for reconnect repair."""
        streams = set(self.receiver.next_sequence) | set(self.receiver.pending)
        streams.update(stream for stream, _ in self._history)
        values: list[dict[str, object]] = []
        remaining_intervals = MAX_TOTAL_INTERVALS
        for stream in sorted(streams, key=lambda value: (value[0], value[1],
                                                         value[2].encode())):
            ranges = self._known_intervals(stream)
            if not ranges:
                continue
            ranges = ranges[-min(MAX_INTERVALS_PER_STREAM,
                                 remaining_intervals):]
            retained = [sequence for (candidate, sequence) in self._history
                        if candidate == stream]
            if retained:
                floor = min(retained)
            elif ranges[-1][1] < 9_007_199_254_740_991:
                # We know the accepted frontier but can no longer serve any
                # item from it. Advertising a floor beyond the high-water mark
                # makes the peer escalate to state sync instead of asking us.
                floor = ranges[-1][1] + 1
            else:
                continue
            values.append({
                "origin": stream[0], "epoch": stream[1], "topic": stream[2],
                "ranges": ranges, "floor": floor,
            })
            remaining_intervals -= len(ranges)
            if len(values) == MAX_STREAMS or remaining_intervals == 0:
                break
        return have_frame(values)

    def connected(self) -> tuple[dict[str, object], dict[str, object]]:
        """Frames that must begin every newly authenticated connection."""
        return (
            interests_frame(self.topics, self.carries, relay_all=self.relay_all),
            self.have(),
        )

    def disconnected(self) -> None:
        """Forget connection-local interests while retaining repair state."""
        self.remote_topics = None
        self.remote_carries = frozenset()
        self.remote_relay_all = False

    def peer_interested(self, topic: str) -> bool:
        """Whether the current peer is eligible for live delivery."""
        return (self.remote_topics is None
                or topic in self.remote_topics
                or topic in self.remote_carries
                or self.remote_relay_all)

    def publish(self, topic: str, payload: bytes) -> dict[str, object]:
        """Sign and retain a publication independently of peer interest."""
        if self.publisher is None:
            raise RuntimeError("this session has no publishing identity")
        publication = self.publisher.publish(topic, payload)
        self._remember(publication)
        return publication

    def _wants(self, frame: Mapping[str, object]) -> tuple[
            list[dict[str, object]], list[tuple[Stream, int, int]]]:
        wants: list[dict[str, object]] = []
        state_sync: list[tuple[Stream, int, int]] = []
        for value in cast(list[dict[str, object]], frame["streams"]):
            stream = (cast(bytes, value["origin"]), cast(int, value["epoch"]),
                      cast(str, value["topic"]))
            if stream[2] not in self.topics:
                continue
            ranges = cast(list[list[int]], value["ranges"])
            if not ranges:
                continue
            floor = cast(int, value["floor"])
            known = self._known_intervals(stream)
            local_through = known[-1][1] if known else -1
            remote_through = ranges[-1][1]
            if floor > 0 and local_through < floor - 1:
                state_sync.append((stream, floor, remote_through))
            for lo, hi in ranges:
                cursor = max(lo, floor)
                for known_lo, known_hi in known:
                    if known_hi < cursor:
                        continue
                    if known_lo > hi:
                        break
                    if known_lo > cursor:
                        self._append_wants(wants, stream, cursor,
                                           min(hi, known_lo - 1))
                        if len(wants) == MAX_WANT_RANGES:
                            return wants, state_sync
                    cursor = max(cursor, known_hi + 1)
                    if cursor > hi:
                        break
                if cursor <= hi:
                    self._append_wants(wants, stream, cursor, hi)
                    if len(wants) == MAX_WANT_RANGES:
                        return wants, state_sync
        return wants, state_sync

    def _append_wants(self, wants: list[dict[str, object]], stream: Stream,
                      lo: int, hi: int) -> None:
        while lo <= hi and len(wants) < MAX_WANT_RANGES:
            end = min(hi, lo + self.max_want_span - 1)
            wants.append({"origin": stream[0], "epoch": stream[1],
                          "topic": stream[2], "lo": lo, "hi": end})
            lo = end + 1

    def receive(self, value: Mapping[str, object]) -> SessionResult:
        """Consume one already-authorized frame and return explicit effects."""
        frame = validate_frame(value)
        frame_type = frame["type"]
        if frame_type == "interests":
            self.remote_topics = frozenset(cast(list[str], frame["topics"]))
            self.remote_carries = frozenset(cast(list[str], frame["carries"]))
            self.remote_relay_all = cast(bool, frame["relay_all"])
            return SessionResult()
        if frame_type == "have":
            wants, state_sync = self._wants(frame)
            outbound = (want_frame(wants),) if wants else ()
            return SessionResult(outbound=outbound,
                                 state_sync_required=tuple(state_sync))
        if frame_type == "want":
            publications: list[dict[str, object]] = []
            for request in cast(list[dict[str, object]], frame["ranges"]):
                stream = (cast(bytes, request["origin"]),
                          cast(int, request["epoch"]),
                          cast(str, request["topic"]))
                hi = min(cast(int, request["hi"]),
                         cast(int, request["lo"]) + self.max_want_span - 1)
                for sequence in range(cast(int, request["lo"]), hi + 1):
                    publication = self._history.get((stream, sequence))
                    if publication is not None:
                        publications.append({**publication, "repair": True})
                        if len(publications) == self.max_want_response:
                            return SessionResult(outbound=tuple(publications))
            return SessionResult(outbound=tuple(publications))
        if frame_type == "gossip":
            received: ReceiveResult = self.receiver.receive(frame)
            self._remember(frame)
            outgoing: tuple[dict[str, object], ...] = ()
            if received.gaps:
                stream = self.receiver.stream(frame)
                ranges = [{"origin": stream[0], "epoch": stream[1],
                           "topic": stream[2], "lo": lo,
                           "hi": min(hi, lo + self.max_want_span - 1)}
                          for lo, hi in received.gaps[:MAX_WANT_RANGES]]
                outgoing = (want_frame(ranges),)
            state_sync_result: tuple[tuple[Stream, int, int], ...] = \
                ((self.receiver.stream(frame), 0,
                  cast(int, frame["seq"])),) \
                if received.state_sync_required else ()
            return SessionResult(outbound=outgoing,
                                 delivered=received.delivered,
                                 state_sync_required=state_sync_result,
                                 duplicate=received.duplicate)
        raise AssertionError("validated dissemination frame has unknown type")


__all__ = ["DisseminationSession", "MAX_WANT_SPAN", "SessionResult"]
