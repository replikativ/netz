"""Ordered, bounded receiving logic for signed Netz publications."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .publication import sign_publication, verify_publication

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
        return (publication["origin"], publication["epoch"], publication["topic"])

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
        sequence = publication["seq"]
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
        gaps = ()
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


__all__ = ["OrderedReceiver", "Publisher", "ReceiveResult", "Stream"]
