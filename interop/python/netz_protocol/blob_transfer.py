"""Transport-independent durable-blob transfer state machines.

One :class:`BlobSender` or :class:`BlobReceiver` owns exactly one transfer
session.  A carrier owns the bounded session table and executes the returned
actions.  Storage completion, receipt signing, receipt verification, and
receipt persistence are explicit boundaries; receiving bytes is never treated
as durable storage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from netz_codec import dumps, loads

from .blob import need_bitmap, needed_indices, validate_manifest, verify_piece
from .replication_wire import (
    MAX_SAFE_INTEGER,
    PERSISTENCE_PROFILES,
    parse_envelope,
    validate_frame,
)

FSYNC = "netz/store/fsync/1"


class BlobTransferError(ValueError):
    """Invalid options, a local completion, or use after session closure."""


@dataclass(frozen=True)
class TransferAction:
    """One effect that must be executed outside the pure session machine."""

    kind: str
    values: dict[str, Any]


def _action(kind: str, **values: Any) -> TransferAction:
    return TransferAction(kind, values)


def _session(value: Any) -> bytes:
    if not isinstance(value, bytes) or len(value) != 16:
        raise BlobTransferError("transfer session must be exactly 16 bytes")
    return value


def _authority(value: Any) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise BlobTransferError("authenticated Authority ID must be 32 bytes")
    return value


def _positive_int(value: Any) -> bool:
    return type(value) is int and 0 < value <= MAX_SAFE_INTEGER


def _now(value: Any) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SAFE_INTEGER:
        raise BlobTransferError("transfer time must be a safe unsigned integer")
    return value


def _copy(value: Any) -> Any:
    """Own or emit a canonical deep copy of protocol data."""
    return loads(dumps(value))


def _persistence_set(value: Any) -> frozenset[str]:
    result = frozenset(value) if isinstance(value, (set, frozenset)) else frozenset()
    if not result or not result.issubset(PERSISTENCE_PROFILES):
        raise BlobTransferError("accepted persistence profiles are invalid")
    return result


def _statement(envelope: dict[str, Any], expected: str) -> dict[str, Any]:
    statement = parse_envelope(envelope)["statement"]
    if statement["type"] != expected:
        raise BlobTransferError(f"expected {expected} statement")
    return statement


def _frame_session(frame: dict[str, Any], session: bytes) -> dict[str, Any]:
    validate_frame(frame)
    if frame["session"] != session:
        raise BlobTransferError("frame belongs to another transfer session")
    return frame


class _Session:
    def __init__(self, peer: Any, remote_authority: bytes, session: bytes,
                 timeout_ms: int) -> None:
        if peer is None:
            raise BlobTransferError("transfer peer is required")
        if not _positive_int(timeout_ms):
            raise BlobTransferError("transfer timeout must be positive")
        self.peer = peer
        self.remote_authority = _authority(remote_authority)
        self.session = _session(session)
        self.timeout_ms = timeout_ms
        self.status = "ready"
        self.abort_code: str | None = None

    def _active(self) -> None:
        if self.status in {"complete", "aborted"}:
            raise BlobTransferError("transfer session is closed")

    def _connection(self, peer: Any, remote_authority: bytes) -> None:
        if peer != self.peer or _authority(remote_authority) != self.remote_authority:
            raise BlobTransferError("frame came from another authenticated connection")

    def _timer(self) -> TransferAction:
        return _action("timer", delay_ms=self.timeout_ms, session=self.session)

    def _send(self, frame: dict[str, Any]) -> TransferAction:
        validate_frame(frame)
        return _action("send", peer=self.peer, frame=_copy(frame))

    def _abort(self, code: str, *, notify: bool = True,
               extra: list[TransferAction] | None = None) -> list[TransferAction]:
        if not isinstance(code, str) or not code:
            code = "invalid-abort"
        self.status = "aborted"
        self.abort_code = code
        actions: list[TransferAction] = []
        if notify:
            actions.append(self._send({
                "v": 1, "type": "blob/abort", "session": self.session,
                "code": code,
            }))
        if extra:
            actions.extend(extra)
        return actions

    def timeout(self) -> list[TransferAction]:
        self._active()
        return self._abort("timeout")

    def disconnected(self, peer: Any) -> list[TransferAction]:
        self._active()
        if peer != self.peer:
            return []
        return self._abort("disconnected", notify=False)


class BlobReceiver(_Session):
    """One resumable receiver with stage/install/receipt backpressure.

    ``offer`` must already have passed signature, controller, layout, lifecycle,
    and authenticated-peer policy.  This class rechecks its closed wire shape,
    target Authority, manifest addresses, lease deadline, and every subsequent
    session/connection fence.
    """

    def __init__(self, authority: bytes, peer: Any, remote_authority: bytes,
                 offer: dict[str, Any], *, now: int = 0,
                 timeout_ms: int = 30_000,
                 max_object_bytes: int = 1 << 30,
                 persistence: str = FSYNC,
                 accepted_persistence: set[str] | frozenset[str] = frozenset({FSYNC})):
        validate_frame(offer)
        if offer["type"] != "blob/offer":
            raise BlobTransferError("receiver requires a blob/offer")
        offer = _copy(offer)
        super().__init__(peer, remote_authority, offer["session"], timeout_ms)
        now = _now(now)
        self.authority = _authority(authority)
        self.accepted_persistence = _persistence_set(accepted_persistence)
        if persistence not in PERSISTENCE_PROFILES:
            raise BlobTransferError("receiver persistence profile is invalid")
        if not _positive_int(max_object_bytes):
            raise BlobTransferError("maximum object size must be positive")
        self.persistence = persistence
        self.manifest = validate_manifest(offer["manifest"])
        self.retention = _statement(offer["retention"], "blob/retention")
        self.manifest_id = self.retention["manifest"]
        self.attempt = offer["attempt"]
        if offer["replica"] != self.authority:
            raise BlobTransferError("offer targets another replica")
        if self.manifest["length"] > max_object_bytes:
            raise BlobTransferError("offered object exceeds the receiver bound")
        if self.retention["retain_until"] <= now:
            raise BlobTransferError("offered retention has expired")
        self._indexes_by_piece: dict[bytes, set[int]] = {}
        for index, address in enumerate(self.manifest["pieces"]):
            self._indexes_by_piece.setdefault(address, set()).add(index)
        self.held: set[int] = set()
        self.needed: set[int] = set()
        self.pending: tuple[int, set[int]] | None = None
        self.receipt: dict[str, Any] | None = None

    def _abort(self, code: str, *, notify: bool = True,
               extra: list[TransferAction] | None = None) -> list[TransferAction]:
        effects = list(extra or [])
        if not any(action.kind == "cleanup_staging" for action in effects):
            effects.append(_action("cleanup_staging", session=self.session,
                                   manifest_id=self.manifest_id))
        return super()._abort(code, notify=notify, extra=effects)

    def start(self) -> list[TransferAction]:
        if self.status != "ready":
            raise BlobTransferError("receiver was already started")
        self.status = "loading"
        return [
            _action("load_staging", session=self.session,
                    manifest_id=self.manifest_id, manifest=_copy(self.manifest)),
            self._timer(),
        ]

    def staging_loaded(self, held: list[bytes] | tuple[bytes, ...],
                       installed: dict[str, Any] | None = None,
                       *, now: int = 0) -> list[TransferAction]:
        now = _now(now)
        if self.status != "loading":
            raise BlobTransferError("receiver is not loading staging state")
        if not isinstance(held, (list, tuple)):
            return self._abort("invalid-staging-inventory")
        try:
            addresses = set(held)
            if (not all(isinstance(item, bytes) and len(item) == 32
                        for item in addresses)
                    or not addresses.issubset(self._indexes_by_piece)):
                raise BlobTransferError("staging inventory is outside the manifest")
            self.held = set().union(*(self._indexes_by_piece[item]
                                      for item in addresses)) if addresses else set()
        except Exception:
            return self._abort("invalid-staging-inventory")

        if installed is not None:
            if (not isinstance(installed, dict)
                    or installed.get("manifest") != self.manifest_id
                    or installed.get("object") != self.manifest["object"]):
                return self._abort("installed-object-mismatch")
            return self._begin_receipt(installed.get("persistence"),
                                       installed.get("stored_at"), now)

        self.needed = set(range(len(self.manifest["pieces"]))) - self.held
        self.status = "receiving"
        return [self._send({
            "v": 1, "type": "blob/need", "session": self.session,
            "manifest": self.manifest_id,
            "need": need_bitmap(self.manifest, addresses),
        })]

    def receive(self, peer: Any, remote_authority: bytes,
                frame: dict[str, Any]) -> list[TransferAction]:
        self._active()
        try:
            self._connection(peer, remote_authority)
            _frame_session(frame, self.session)
        except Exception:
            return self._abort("connection-or-session-mismatch")
        kind = frame["type"]
        if kind == "blob/piece":
            return self._piece(frame)
        if kind == "blob/commit":
            return self._commit(frame)
        if kind == "blob/abort":
            return self._abort(frame["code"], notify=False,
                               extra=[_action("cleanup_staging",
                                              session=self.session,
                                              manifest_id=self.manifest_id)])
        return self._abort("wrong-frame")

    def _piece(self, frame: dict[str, Any]) -> list[TransferAction]:
        if self.status != "receiving":
            return self._abort("unknown-session")
        if frame["manifest"] != self.manifest_id:
            return self._abort("manifest-mismatch")
        index = frame["index"]
        if index in self.held:
            through = self._contiguous_through()
            return [] if through < 0 else [self._send({
                "v": 1, "type": "blob/ack", "session": self.session,
                "manifest": self.manifest_id, "through": through,
            })]
        if self.pending is not None:
            return self._abort("piece-before-stage")
        if index not in self.needed:
            return self._abort("unrequested-piece")
        try:
            valid = verify_piece(self.manifest, index, frame["data"])
        except Exception:
            valid = False
        if not valid:
            return self._abort("invalid-piece")
        address = self.manifest["pieces"][index]
        indices = set(self._indexes_by_piece[address])
        self.pending = (index, indices)
        return [_action("stage_piece", session=self.session,
                        manifest_id=self.manifest_id,
                        manifest=_copy(self.manifest),
                        index=index, piece_id=address, data=frame["data"])]

    def piece_staged(self, index: int, result: str) -> list[TransferAction]:
        if self.status != "receiving" or self.pending is None \
                or self.pending[0] != index:
            raise BlobTransferError("unexpected piece staging completion")
        if result != "ok":
            return self._abort("stage-failed")
        _, indices = self.pending
        self.held.update(indices)
        self.needed.difference_update(indices)
        self.pending = None
        through = self._contiguous_through()
        return [] if through < 0 else [self._send({
            "v": 1, "type": "blob/ack", "session": self.session,
            "manifest": self.manifest_id, "through": through,
        })]

    def _contiguous_through(self) -> int:
        index = 0
        while index < len(self.manifest["pieces"]) and index in self.held:
            index += 1
        return index - 1

    def _commit(self, frame: dict[str, Any]) -> list[TransferAction]:
        if self.status != "receiving":
            return self._abort("unknown-session")
        if frame["manifest"] != self.manifest_id:
            return self._abort("manifest-mismatch")
        if (self.pending is not None or self.needed
                or frame["count"] != len(self.manifest["pieces"])):
            return self._abort("incomplete")
        self.status = "installing"
        return [_action("install_object", session=self.session,
                        manifest_id=self.manifest_id,
                        manifest=_copy(self.manifest),
                        retention=_copy(self.retention),
                        persistence=self.persistence)]

    def installed(self, result: str, persistence: str | None = None,
                  stored_at: int | None = None, *, now: int = 0) -> list[TransferAction]:
        now = _now(now)
        if self.status != "installing":
            raise BlobTransferError("unexpected object installation completion")
        if result != "ok":
            return self._abort("install-failed")
        return self._begin_receipt(persistence, stored_at, now)

    def _begin_receipt(self, persistence: Any, stored_at: Any,
                       now: int) -> list[TransferAction]:
        retain_until = self.retention["retain_until"]
        if persistence not in self.accepted_persistence:
            return self._abort("unacceptable-persistence")
        if (type(stored_at) is not int
                or not 0 <= stored_at <= retain_until <= MAX_SAFE_INTEGER):
            return self._abort("invalid-stored-at")
        if now >= retain_until:
            return self._abort("lease-expired")
        self.receipt = {
            "v": 1, "type": "blob/stored",
            "cluster": self.retention["cluster"],
            "privacy_domain": self.retention["privacy_domain"],
            "retention_revision": self.retention["revision"],
            "manifest": self.manifest_id,
            "object": self.retention["object"],
            "layout_epoch": self.retention["layout_epoch"],
            "replica": self.authority,
            "attempt": self.attempt,
            "persistence": persistence,
            "stored_at": stored_at,
            "retain_until": retain_until,
            "signer": self.authority,
        }
        self.status = "signing-receipt"
        return [_action("sign_receipt", session=self.session,
                        statement=_copy(self.receipt))]

    def receipt_signed(self, envelope: dict[str, Any]) -> list[TransferAction]:
        if self.status != "signing-receipt" or self.receipt is None:
            raise BlobTransferError("unexpected signed receipt")
        try:
            actual = _statement(envelope, "blob/stored")
            exact = dumps(actual) == dumps(self.receipt)
        except Exception:
            exact = False
        if not exact:
            return self._abort("invalid-receipt")
        self.status = "complete"
        envelope = _copy(envelope)
        event = {
            "type": "replication/installed",
            "session": self.session,
            "object_id": actual["object"].hex(),
            "manifest_id": self.manifest_id.hex(),
            "replica_id": actual["replica"].hex(),
            "layout_epoch": actual["layout_epoch"],
            "attempt": actual["attempt"],
            "persistence": actual["persistence"],
            "receipt": envelope,
        }
        return [
            self._send({"v": 1, "type": "blob/stored",
                        "session": self.session, "receipt": envelope}),
            _action("cleanup_staging", session=self.session,
                    manifest_id=self.manifest_id),
            _action("replica_installed", event=event),
        ]

    def timeout(self) -> list[TransferAction]:
        self._active()
        return self._abort("timeout", extra=[
            _action("cleanup_staging", session=self.session,
                    manifest_id=self.manifest_id)
        ])


class BlobSender(_Session):
    """One bounded sender with one piece in flight and durable receipts.

    Construction happens only after the controller's exact outgoing intent is
    durable.  ``verified_stored`` accepts only a receipt envelope whose
    signature and authenticated remote signer were already checked by the
    caller; the method rechecks all operation fields before persistence.
    """

    def __init__(self, peer: Any, remote_authority: bytes,
                 offer: dict[str, Any], *, now: int = 0,
                 timeout_ms: int = 30_000,
                 accepted_persistence: set[str] | frozenset[str] = frozenset({FSYNC})):
        validate_frame(offer)
        if offer["type"] != "blob/offer":
            raise BlobTransferError("sender requires a blob/offer")
        offer = _copy(offer)
        super().__init__(peer, remote_authority, offer["session"], timeout_ms)
        now = _now(now)
        if offer["replica"] != self.remote_authority:
            raise BlobTransferError("offer does not target the authenticated peer")
        self.accepted_persistence = _persistence_set(accepted_persistence)
        self.offer = offer
        self.manifest = validate_manifest(offer["manifest"])
        self.retention = _statement(offer["retention"], "blob/retention")
        self.manifest_id = self.retention["manifest"]
        self.attempt = offer["attempt"]
        if self.retention["retain_until"] <= now:
            raise BlobTransferError("offered retention has expired")
        self.need: bytes | None = None
        self.remaining: list[int] = []
        self.current: int | None = None
        self.acked_through = -1
        self.controller_event: dict[str, Any] | None = None

    def start(self) -> list[TransferAction]:
        if self.status != "ready":
            raise BlobTransferError("sender was already started")
        self.status = "awaiting-need"
        return [self._send(self.offer), self._timer()]

    def receive(self, peer: Any, remote_authority: bytes,
                frame: dict[str, Any]) -> list[TransferAction]:
        self._active()
        try:
            self._connection(peer, remote_authority)
            _frame_session(frame, self.session)
        except Exception:
            return self._abort("connection-or-session-mismatch",
                               extra=[self._failure("connection-or-session-mismatch")])
        kind = frame["type"]
        if kind == "blob/need":
            return self._need(frame)
        if kind == "blob/ack":
            return self._ack(frame)
        if kind == "blob/abort":
            return self._abort(frame["code"], notify=False,
                               extra=[self._failure(frame["code"])])
        return self._abort("wrong-frame", extra=[self._failure("wrong-frame")])

    def _load(self, index: int) -> TransferAction:
        self.current = index
        self.status = "loading-piece"
        return _action("load_piece", session=self.session,
                       manifest_id=self.manifest_id, index=index,
                       piece_id=self.manifest["pieces"][index])

    def _commit(self) -> TransferAction:
        self.current = None
        self.status = "awaiting-receipt"
        return self._send({
            "v": 1, "type": "blob/commit", "session": self.session,
            "manifest": self.manifest_id,
            "count": len(self.manifest["pieces"]),
        })

    def _need(self, frame: dict[str, Any]) -> list[TransferAction]:
        if frame["manifest"] != self.manifest_id:
            return self._abort("manifest-mismatch",
                               extra=[self._failure("manifest-mismatch")])
        if self.status != "awaiting-need":
            if self.need == frame["need"]:
                return []
            return self._abort("unexpected-need",
                               extra=[self._failure("unexpected-need")])
        try:
            self.remaining = needed_indices(self.manifest, frame["need"])
        except Exception:
            return self._abort("invalid-need", extra=[self._failure("invalid-need")])
        self.need = frame["need"]
        return [self._load(self.remaining[0])] if self.remaining else [self._commit()]

    def piece_loaded(self, index: int, result: str,
                     data: bytes | None = None) -> list[TransferAction]:
        if self.status != "loading-piece" or self.current != index:
            raise BlobTransferError("unexpected local piece completion")
        if result != "ok":
            return self._abort("piece-load-failed",
                               extra=[self._failure("piece-load-failed")])
        try:
            valid = isinstance(data, bytes) and verify_piece(self.manifest, index, data)
        except Exception:
            valid = False
        if not valid:
            return self._abort("invalid-local-piece",
                               extra=[self._failure("invalid-local-piece")])
        self.status = "awaiting-ack"
        # The bytes exist only in this returned effect, never in sender state.
        return [self._send({
            "v": 1, "type": "blob/piece", "session": self.session,
            "manifest": self.manifest_id, "index": index, "data": data,
        })]

    def _ack(self, frame: dict[str, Any]) -> list[TransferAction]:
        if frame["manifest"] != self.manifest_id:
            return self._abort("manifest-mismatch",
                               extra=[self._failure("manifest-mismatch")])
        through = frame["through"]
        if through >= len(self.manifest["pieces"]):
            return self._abort("invalid-ack", extra=[self._failure("invalid-ack")])
        if through <= self.acked_through:
            return []
        if self.status == "awaiting-ack" and self.current is not None \
                and through < self.current:
            return []
        if self.status != "awaiting-ack":
            return self._abort("unexpected-ack",
                               extra=[self._failure("unexpected-ack")])
        self.acked_through = through
        self.remaining = [index for index in self.remaining if index > through]
        return [self._load(self.remaining[0])] if self.remaining else [self._commit()]

    def verified_stored(self, peer: Any, remote_authority: bytes,
                        frame: dict[str, Any]) -> list[TransferAction]:
        """Accept one already signature-verified remote storage receipt."""
        self._active()
        try:
            self._connection(peer, remote_authority)
            _frame_session(frame, self.session)
            statement = _statement(frame["receipt"], "blob/stored")
        except Exception:
            return self._abort("invalid-receipt",
                               extra=[self._failure("invalid-receipt")])
        # An installed replica may answer the offer directly. Exact receipt
        # binding and durable recording remain mandatory below.
        if self.status not in {"awaiting-need", "awaiting-receipt"}:
            if (self.status == "persisting-receipt"
                    and self.controller_event is not None
                    and dumps(frame["receipt"])
                    == dumps(self.controller_event["receipt"])):
                return []
            return self._abort("unexpected-receipt",
                               extra=[self._failure("unexpected-receipt")])
        if not self._receipt_matches(statement):
            return self._abort("receipt-mismatch",
                               extra=[self._failure("receipt-mismatch")])
        self.controller_event = {
            "type": "replication/stored",
            "object_id": statement["object"].hex(),
            "replica_id": statement["replica"].hex(),
            "layout_epoch": statement["layout_epoch"],
            "attempt": statement["attempt"],
            "persistence": statement["persistence"],
            "receipt": frame["receipt"],
            "manifest_id": self.manifest_id.hex(),
        }
        self.status = "persisting-receipt"
        return [_action("persist_receipt", session=self.session,
                        event=_copy(self.controller_event))]

    def _receipt_matches(self, statement: dict[str, Any]) -> bool:
        return (
            statement["replica"] == self.remote_authority
            and statement["signer"] == self.remote_authority
            and statement["cluster"] == self.retention["cluster"]
            and statement["privacy_domain"] == self.retention["privacy_domain"]
            and statement["retention_revision"] == self.retention["revision"]
            and statement["manifest"] == self.manifest_id
            and statement["object"] == self.manifest["object"]
            and statement["layout_epoch"] == self.retention["layout_epoch"]
            and statement["attempt"] == self.attempt
            and statement["retain_until"] == self.retention["retain_until"]
            and statement["persistence"] in self.accepted_persistence
        )

    def receipt_persisted(self, result: str) -> list[TransferAction]:
        if self.status != "persisting-receipt" or self.controller_event is None:
            raise BlobTransferError("unexpected receipt persistence completion")
        if result != "ok":
            return self._abort("receipt-persist-failed", notify=False,
                               extra=[self._failure("receipt-persist-failed")])
        self.status = "complete"
        return [_action("replication_stored", event=_copy(self.controller_event))]

    def _failure(self, code: str) -> TransferAction:
        return _action(
            "transfer_failed",
            event={
                "session": self.session,
                "object_id": self.manifest["object"].hex(),
                "manifest_id": self.manifest_id.hex(),
                "replica_id": self.remote_authority.hex(),
                "layout_epoch": self.retention["layout_epoch"],
                "attempt": self.attempt,
                "code": code,
            },
        )

    def timeout(self) -> list[TransferAction]:
        self._active()
        return self._abort("timeout", extra=[self._failure("timeout")])

    def disconnected(self, peer: Any) -> list[TransferAction]:
        self._active()
        if peer != self.peer:
            return []
        return self._abort("disconnected", notify=False,
                           extra=[self._failure("disconnected")])


__all__ = [
    "BlobReceiver", "BlobSender", "BlobTransferError", "TransferAction",
]
