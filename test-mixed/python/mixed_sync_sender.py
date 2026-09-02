#!/usr/bin/env python3
"""Serve one portable join-semilattice synchronization to the JVM."""

from __future__ import annotations

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    JoinSetProfile,
    SYNC_CAPABILITY,
    SyncSender,
    decode_extension_frame,
    encode_extension_frame,
    genesis,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity, open_secure_websocket

TOPIC = "demo/join"


def _identity() -> LocalIdentity:
    private = bytes([19]) * 32
    public = (ed25519.Ed25519PrivateKey.from_private_bytes(private)
              .public_key().public_bytes_raw())
    value = genesis([public], 1, [public], bytes([51]) * 32)
    capabilities = DEFAULT_CAPABILITIES + (
        WIRE_PROFILE_CAPABILITY,
        EXTENSION_CARRIER_CAPABILITY,
        SYNC_CAPABILITY,
    )
    return LocalIdentity(
        genesis=value, operational_private=private,
        capabilities=capabilities, required=capabilities,
    )


async def serve(uri: str, remote_authority: bytes) -> None:
    socket = await open_secure_websocket(
        uri, identity=_identity(), expected_authority=remote_authority
    )
    selected = frozenset(socket.peer.capabilities)
    profile = JoinSetProfile()
    try:
        encoded = await asyncio.wait_for(socket.recv_frame(), timeout=15)
        capability, frame = decode_extension_frame(
            encoded, selected_capabilities=selected
        )
        if capability != SYNC_CAPABILITY or not isinstance(frame, dict):
            raise RuntimeError("JVM did not open the expected sync extension")
        # Protocol support is not read authorization. The Noise dial already
        # pins the JVM Authority; pin its only permitted topic here as well.
        if frame.get("topic") != TOPIC or frame.get("profile") != profile.name:
            raise RuntimeError("JVM requested an unauthorized sync scope")
        sender = SyncSender.from_open(frame, profile, {"python"}, [])
        pending = sender.start()
        while pending or sender.status == "synchronizing":
            for reply in pending:
                await socket.send_frame(encode_extension_frame(
                    SYNC_CAPABILITY, reply,
                    selected_capabilities=selected,
                ))
            if sender.status != "synchronizing":
                break
            encoded = await asyncio.wait_for(socket.recv_frame(), timeout=15)
            capability, frame = decode_extension_frame(
                encoded, selected_capabilities=selected
            )
            if capability != SYNC_CAPABILITY or not isinstance(frame, dict):
                raise RuntimeError("JVM returned an unexpected extension frame")
            pending = sender.receive(frame)
        if sender.status != "synchronized":
            raise RuntimeError(f"synchronization ended as {sender.status}")
    finally:
        await socket.websocket.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    args = parser.parse_args()
    remote_authority = bytes.fromhex(args.remote_authority)
    if len(remote_authority) != 32:
        parser.error("--remote-authority must be 32 bytes of hexadecimal")
    asyncio.run(serve(args.uri, remote_authority))
    print("NETZ_MIXED_SYNC_SERVED")


if __name__ == "__main__":
    main()
