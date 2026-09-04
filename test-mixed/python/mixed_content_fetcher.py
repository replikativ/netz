#!/usr/bin/env python3
"""Fetch one portable raw-CID block from the JVM service."""

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    CONTENT_CAPABILITY,
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    ContentRequester,
    authority_id,
    decode_extension_frame,
    encode_extension_frame,
    genesis,
    raw_cid,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity, open_secure_websocket

DATA = b"portable content over authenticated Kabel"


def identity() -> LocalIdentity:
    private = bytes([23]) * 32
    public = (ed25519.Ed25519PrivateKey.from_private_bytes(private)
              .public_key().public_bytes_raw())
    value = genesis([public], 1, [public], bytes([55]) * 32)
    capabilities = DEFAULT_CAPABILITIES + (
        WIRE_PROFILE_CAPABILITY,
        EXTENSION_CARRIER_CAPABILITY,
        CONTENT_CAPABILITY,
    )
    return LocalIdentity(genesis=value, operational_private=private,
                         capabilities=capabilities, required=capabilities)


async def fetch(uri: str, remote_authority: bytes) -> None:
    local = identity()
    socket = await open_secure_websocket(
        uri, identity=local, expected_authority=remote_authority)
    selected = frozenset(socket.peer.capabilities)
    requester = ContentRequester(timeout_ms=15_000)
    request = bytes(range(16))
    cid = raw_cid(DATA)
    want = requester.want(remote_authority, cid, now_ms=0, request=request)
    try:
        await socket.send_frame(encode_extension_frame(
            CONTENT_CAPABILITY, want, selected_capabilities=selected))
        capability, response = decode_extension_frame(
            await asyncio.wait_for(socket.recv_frame(), timeout=15),
            selected_capabilities=selected)
        if capability != CONTENT_CAPABILITY:
            raise RuntimeError("JVM replied under the wrong capability")
        result = requester.receive(remote_authority, response, now_ms=1)
        if result is None or result.data != DATA:
            raise RuntimeError("JVM returned the wrong content block")
    finally:
        await socket.websocket.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    args = parser.parse_args()
    remote = bytes.fromhex(args.remote_authority)
    asyncio.run(fetch(args.uri, remote))
    print("NETZ_MIXED_CONTENT_OK")


if __name__ == "__main__":
    main()
