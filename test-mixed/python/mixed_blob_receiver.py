#!/usr/bin/env python3
"""Fetch one JVM blob into the installed Python fsync repository."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    BlobReceiver,
    FileBlobStore,
    TransferAction,
    authority_id,
    decode_extension_frame,
    dumps,
    encode_extension_frame,
    genesis,
    split_blob,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.replication_wire import (
    CAPABILITY as BLOB_CAPABILITY,
    parse_envelope,
    sign_statement,
    verify_envelope,
)
from netz_protocol.secure import LocalIdentity, open_secure_websocket

PAYLOAD = bytes(index % 251 for index in range((1 << 20) + 137))
PIECE_SIZE = 1 << 20
NOW = 1_000
PEER = "jvm"


class _IntentionalInterruption(Exception):
    """Stop after a durable piece to exercise process-level resume."""


def _identity() -> tuple[LocalIdentity, bytes]:
    private = bytes([17]) * 32
    public = (ed25519.Ed25519PrivateKey.from_private_bytes(private)
              .public_key().public_bytes_raw())
    value = genesis([public], 1, [public], bytes([49]) * 32)
    capabilities = DEFAULT_CAPABILITIES + (
        WIRE_PROFILE_CAPABILITY,
        EXTENSION_CARRIER_CAPABILITY,
        BLOB_CAPABILITY,
    )
    return LocalIdentity(
        genesis=value, operational_private=private,
        capabilities=capabilities, required=capabilities,
    ), authority_id(value)


def _retention(identity: LocalIdentity, authority: bytes,
               split: dict[str, Any]) -> dict[str, Any]:
    statement = {
        "v": 1, "type": "blob/retention",
        "cluster": bytes(range(16)),
        "privacy_domain": bytes(range(16, 32)),
        "revision": 1, "manifest": split["manifest_id"],
        "object": split["object_id"], "layout_epoch": 1,
        "replicas": 1, "write_acks": 1, "minimum_domains": {},
        "issued_at": NOW, "retain_until": 5_000, "signer": authority,
    }
    return sign_statement(
        identity.genesis, identity.operational_private, statement
    )


def _persist_admission(root: Path, retention: dict[str, Any]) -> bytes:
    """Durably record this test controller's exact admitted intent."""
    root.mkdir(parents=True, exist_ok=True)
    encoded = dumps(retention)
    target = root / ".netz-test-lifecycle-admission.cbor"
    temporary = root / ".netz-test-lifecycle-admission.cbor.tmp"
    try:
        with temporary.open("wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return encoded


async def _wait_for_completion(path: Path) -> None:
    for _ in range(1_000):
        if path.is_file():
            return
        await asyncio.sleep(0.01)
    raise RuntimeError("JVM did not journal the replica receipt")


async def transfer(uri: str, remote_authority: bytes, store_path: Path,
                   completion_path: Path, attempt: int = 0,
                   stop_after_pieces: int = 0) -> tuple[str, str]:
    identity, local_authority = _identity()
    split = split_blob(PAYLOAD, PIECE_SIZE)
    retention = _retention(identity, local_authority, split)
    admitted_retention = _persist_admission(store_path, retention)
    fetch = {
        "v": 1, "type": "blob/fetch", "session": attempt.to_bytes(16, "big"),
        "attempt": attempt, "retention": retention,
    }
    socket = await open_secure_websocket(
        uri, identity=identity, expected_authority=remote_authority
    )
    selected = frozenset(socket.peer.capabilities)
    receiver: BlobReceiver | None = None
    completed: dict[str, Any] | None = None
    staged_pieces = 0

    # The JVM harness supplies an existing, exclusively owned test root.
    async with FileBlobStore(store_path, durable_ancestor=store_path,
                             clock=lambda: 1_200) as store:
        async def apply(actions: list[TransferAction]) -> None:
            nonlocal completed, staged_pieces
            if receiver is None:
                raise RuntimeError("receiver is not initialized")
            pending = list(actions)
            while pending:
                action = pending.pop(0)
                values = action.values
                if action.kind == "send":
                    await socket.send_frame(encode_extension_frame(
                        BLOB_CAPABILITY, values["frame"],
                        selected_capabilities=selected,
                    ))
                elif action.kind == "load_staging":
                    loaded = await store.load_staging(
                        values["session"], values["manifest_id"],
                        values["manifest"],
                    )
                    pending.extend(receiver.staging_loaded(**loaded, now=NOW))
                elif action.kind == "stage_piece":
                    stage_result = await store.stage_piece(
                        values["session"], values["manifest_id"],
                        values["manifest"], values["index"],
                        values["piece_id"], values["data"],
                    )
                    pending.extend(receiver.piece_staged(
                        values["index"], stage_result
                    ))
                    staged_pieces += 1
                    if (stop_after_pieces > 0
                            and staged_pieces >= stop_after_pieces):
                        # Deliberately stop after fsync but before ACK. A fresh
                        # process/session must discover this addressed piece.
                        raise _IntentionalInterruption
                elif action.kind == "install_object":
                    install_result = await store.install_object(
                        values["session"], values["manifest_id"],
                        values["manifest"], values["retention"],
                        values["persistence"],
                    )
                    pending.extend(receiver.installed(
                        install_result["result"],
                        install_result["persistence"],
                        install_result["stored_at"], now=1_200,
                    ))
                elif action.kind == "sign_receipt":
                    envelope = sign_statement(
                        identity.genesis, identity.operational_private,
                        values["statement"],
                    )
                    pending.extend(receiver.receipt_signed(envelope))
                elif action.kind == "cleanup_staging":
                    await store.cleanup_session(
                        values["session"], values["manifest_id"]
                    )
                elif action.kind == "replica_installed":
                    completed = values["event"]
                elif action.kind == "timer":
                    continue
                else:
                    raise RuntimeError(
                        f"unexpected receiver action: {action!r}"
                    )

        try:
            await socket.send_frame(encode_extension_frame(
                BLOB_CAPABILITY, fetch, selected_capabilities=selected
            ))
            while receiver is None or receiver.status not in {"complete", "aborted"}:
                encoded = await asyncio.wait_for(socket.recv_frame(), timeout=15)
                capability, frame = decode_extension_frame(
                    encoded, selected_capabilities=selected
                )
                if capability != BLOB_CAPABILITY or not isinstance(frame, dict):
                    raise RuntimeError("JVM returned an unexpected extension frame")
                if receiver is None:
                    envelope = frame.get("retention")
                    if (frame.get("type") != "blob/offer"
                            or frame.get("session") != fetch["session"]
                            or frame.get("attempt") != fetch["attempt"]
                            or not isinstance(envelope, dict)
                            or dumps(envelope) != admitted_retention
                            or not verify_envelope(envelope)
                            or parse_envelope(envelope)["statement"]["signer"]
                            != local_authority
                            or frame.get("manifest") != split["manifest"]):
                        raise RuntimeError("JVM returned an invalid fetch offer")
                    receiver = BlobReceiver(
                        local_authority, PEER, remote_authority, frame, now=NOW
                    )
                    await apply(receiver.start())
                else:
                    await apply(receiver.receive(
                        PEER, remote_authority, frame
                    ))
            if receiver.status != "complete" or completed is None:
                raise RuntimeError(f"blob transfer ended as {receiver.status}")
            value = await store.read_installed(split["manifest_id"], len(PAYLOAD))
            if value != PAYLOAD:
                raise RuntimeError("installed Python object does not match")
            # `blob/stored` is terminal in draft-00, so the wire protocol has
            # no receipt ACK. The conformance controller supplies this local
            # signal to keep the socket open until the JVM journal callback.
            await _wait_for_completion(completion_path)
            return "complete", split["manifest_id"].hex()
        except _IntentionalInterruption:
            return "interrupted", split["manifest_id"].hex()
        finally:
            await socket.websocket.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    parser.add_argument("--store", required=True, type=Path)
    parser.add_argument("--completion", required=True, type=Path)
    parser.add_argument("--attempt", type=int, default=0)
    parser.add_argument("--stop-after-pieces", type=int, default=0)
    args = parser.parse_args()
    authority = bytes.fromhex(args.remote_authority)
    if len(authority) != 32:
        parser.error("--remote-authority must be 32 bytes of hexadecimal")
    if not 0 <= args.attempt <= (1 << 53) - 1:
        parser.error("--attempt must be a safe unsigned integer")
    if args.stop_after_pieces < 0:
        parser.error("--stop-after-pieces must not be negative")
    status, manifest = asyncio.run(transfer(
        args.uri, authority, args.store, args.completion,
        args.attempt, args.stop_after_pieces,
    ))
    marker = ("NETZ_MIXED_FETCH_OK" if status == "complete"
              else "NETZ_MIXED_FETCH_INTERRUPTED")
    print(f"{marker} {manifest}", flush=True)


if __name__ == "__main__":
    main()
