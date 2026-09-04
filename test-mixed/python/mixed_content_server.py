#!/usr/bin/env python3
"""Serve one portable raw-CID block to a JVM requester."""

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    CONTENT_CAPABILITY,
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    ContentService,
    authority_id,
    decode_extension_frame,
    encode_extension_frame,
    genesis,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity, open_secure_websocket

DATA = b"portable content over authenticated Kabel"


def identity() -> tuple[LocalIdentity, bytes]:
    private = bytes([29]) * 32
    public = (ed25519.Ed25519PrivateKey.from_private_bytes(private)
              .public_key().public_bytes_raw())
    value = genesis([public], 1, [public], bytes([61]) * 32)
    capabilities = DEFAULT_CAPABILITIES + (
        WIRE_PROFILE_CAPABILITY,
        EXTENSION_CARRIER_CAPABILITY,
        CONTENT_CAPABILITY,
    )
    return (LocalIdentity(genesis=value, operational_private=private,
                          capabilities=capabilities, required=capabilities),
            authority_id(value))


async def serve(uri: str, remote_authority: bytes) -> None:
    local, local_authority = identity()
    socket = await open_secure_websocket(
        uri, identity=local, expected_authority=remote_authority)
    selected = frozenset(socket.peer.capabilities)
    reads = 0

    def authorize(context):
        return (context["peer_authority"] == remote_authority
                and context["op"] == "content/read")

    def get_block(_cid):
        nonlocal reads
        reads += 1
        return DATA

    service = ContentService(authorize=authorize, get_block=get_block)
    print(f"NETZ_MIXED_CONTENT_READY {local_authority.hex()}", flush=True)
    try:
        capability, request = decode_extension_frame(
            await asyncio.wait_for(socket.recv_frame(), timeout=15),
            selected_capabilities=selected)
        if capability != CONTENT_CAPABILITY:
            raise RuntimeError("JVM requested under the wrong capability")
        response = await service.receive(remote_authority, request)
        await socket.send_frame(encode_extension_frame(
            CONTENT_CAPABILITY, response, selected_capabilities=selected))
        if reads != 1:
            raise RuntimeError("content backend was not read exactly once")
        print("NETZ_MIXED_CONTENT_SERVED", flush=True)
    finally:
        await socket.websocket.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    args = parser.parse_args()
    asyncio.run(serve(args.uri, bytes.fromhex(args.remote_authority)))


if __name__ == "__main__":
    main()
