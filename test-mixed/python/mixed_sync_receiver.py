#!/usr/bin/env python3
"""Pull one portable join-semilattice synchronization from the JVM."""

from __future__ import annotations

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    JoinSetProfile,
    SYNC_CAPABILITY,
    SyncReceiver,
    authority_id,
    decode_extension_frame,
    encode_extension_frame,
    genesis,
    open_frame,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity, open_secure_websocket

TOPIC = "demo/join"
PEER = "jvm"


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


async def synchronize(uri: str, remote_authority: bytes) -> set[str]:
    identity = _identity()
    socket = await open_secure_websocket(
        uri, identity=identity, expected_authority=remote_authority
    )
    selected = frozenset(socket.peer.capabilities)
    session = bytes(range(96, 112))
    profile = JoinSetProfile()
    receiver = SyncReceiver(session, TOPIC, profile, {"python"})
    try:
        await socket.send_frame(encode_extension_frame(
            SYNC_CAPABILITY,
            open_frame(session, TOPIC, profile, receiver.value),
            selected_capabilities=selected,
        ))
        while receiver.status == "synchronizing":
            encoded = await asyncio.wait_for(socket.recv_frame(), timeout=15)
            capability, frame = decode_extension_frame(
                encoded, selected_capabilities=selected
            )
            if capability != SYNC_CAPABILITY or not isinstance(frame, dict):
                raise RuntimeError("JVM returned an unexpected extension frame")
            if frame.get("type") == "sync/abort":
                raise RuntimeError(f"JVM aborted synchronization: {frame.get('code')}")
            reply = receiver.receive(frame)
            if reply is not None:
                await socket.send_frame(encode_extension_frame(
                    SYNC_CAPABILITY, reply,
                    selected_capabilities=selected,
                ))
        if receiver.status != "synchronized":
            raise RuntimeError(f"synchronization ended as {receiver.status}")
        return receiver.value
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
    value = asyncio.run(synchronize(args.uri, remote_authority))
    print(f"NETZ_MIXED_SYNC_OK {','.join(sorted(value))}", flush=True)


if __name__ == "__main__":
    main()
