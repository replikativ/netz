#!/usr/bin/env python3
"""Send one durable blob from an installed Python package to a JVM test peer.

This is an executable cross-runtime conformance peer, not a mock transport. It
uses the public Python protocol state machine and secure WebSocket adapter while
the JVM process owns the receiver runtime and filesystem store.
"""

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
    BlobSender,
    TransferAction,
    authority_id,
    dumps,
    encode_extension_frame,
    decode_extension_frame,
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
        genesis=value,
        operational_private=private,
        capabilities=capabilities,
        required=capabilities,
    ), authority_id(value)


def _offer(identity: LocalIdentity, local_authority: bytes,
           remote_authority: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    split = split_blob(PAYLOAD, PIECE_SIZE)
    retention = {
        "v": 1,
        "type": "blob/retention",
        "cluster": bytes(range(16)),
        "privacy_domain": bytes(range(16, 32)),
        "revision": 1,
        "manifest": split["manifest_id"],
        "object": split["object_id"],
        "layout_epoch": 1,
        "replicas": 1,
        "write_acks": 1,
        "minimum_domains": {},
        "issued_at": NOW,
        "retain_until": 5_000,
        "signer": local_authority,
    }
    return split, {
        "v": 1,
        "type": "blob/offer",
        "session": bytes(range(16)),
        "attempt": 0,
        "replica": remote_authority,
        "retention": sign_statement(
            identity.genesis, identity.operational_private, retention
        ),
        "manifest": split["manifest"],
    }


def _persist_receipt(path: Path, event: dict[str, Any]) -> None:
    """Atomically fsync the receipt before reporting sender completion."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    encoded = dumps(event)
    with temporary.open("wb") as output:
        output.write(encoded)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


async def transfer(uri: str, remote_authority: bytes,
                   receipt_path: Path) -> str:
    identity, local_authority = _identity()
    split, offer = _offer(identity, local_authority, remote_authority)
    socket = await open_secure_websocket(
        uri, identity=identity, expected_authority=remote_authority
    )
    selected = frozenset(socket.peer.capabilities)
    sender = BlobSender(PEER, remote_authority, offer, now=NOW)
    pieces = {piece["index"]: piece for piece in split["pieces"]}
    completed: dict[str, Any] | None = None

    async def apply(actions: list[TransferAction]) -> None:
        nonlocal completed
        pending = list(actions)
        while pending:
            action = pending.pop(0)
            values = action.values
            if action.kind == "send":
                await socket.send_frame(encode_extension_frame(
                    BLOB_CAPABILITY, values["frame"],
                    selected_capabilities=selected,
                ))
            elif action.kind == "load_piece":
                piece = pieces[values["index"]]
                pending.extend(sender.piece_loaded(
                    piece["index"], "ok", piece["data"]
                ))
            elif action.kind == "persist_receipt":
                _persist_receipt(receipt_path, values["event"])
                pending.extend(sender.receipt_persisted("ok"))
            elif action.kind == "replication_stored":
                completed = values["event"]
            elif action.kind == "timer":
                # asyncio.wait_for below owns the executable test deadline.
                continue
            elif action.kind == "transfer_failed":
                raise RuntimeError(f"blob transfer failed: {values['event']!r}")
            else:
                raise RuntimeError(f"unexpected sender action: {action!r}")

    try:
        await apply(sender.start())
        while sender.status not in {"complete", "aborted"}:
            encoded = await asyncio.wait_for(socket.recv_frame(), timeout=15)
            capability, frame = decode_extension_frame(
                encoded, selected_capabilities=selected
            )
            if capability != BLOB_CAPABILITY or not isinstance(frame, dict):
                raise RuntimeError("JVM returned an unexpected extension frame")
            if frame.get("type") == "blob/stored":
                receipt = frame.get("receipt")
                if (not isinstance(receipt, dict)
                        or not verify_envelope(receipt)
                        or parse_envelope(receipt)["statement"]["signer"]
                        != remote_authority):
                    raise RuntimeError("JVM returned an invalid storage receipt")
                actions = sender.verified_stored(
                    PEER, remote_authority, frame
                )
            else:
                actions = sender.receive(PEER, remote_authority, frame)
            await apply(actions)
        if sender.status != "complete" or completed is None:
            raise RuntimeError(f"blob transfer ended as {sender.status}")
        return split["manifest_id"].hex()
    finally:
        await socket.websocket.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    authority = bytes.fromhex(args.remote_authority)
    if len(authority) != 32:
        parser.error("--remote-authority must be 32 bytes of hexadecimal")
    manifest = asyncio.run(transfer(args.uri, authority, args.receipt))
    print(f"NETZ_MIXED_OK {manifest}", flush=True)


if __name__ == "__main__":
    main()
