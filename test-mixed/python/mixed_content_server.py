#!/usr/bin/env python3
"""Serve one portable raw-CID block to a JVM requester."""

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    CONTENT_CAPABILITY,
    DisseminationClient,
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    ContentService,
    authority_id,
    genesis,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity

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
    reads = 0
    served = asyncio.Event()

    def authorize(context):
        return (context["peer_authority"] == remote_authority
                and context["op"] == "content/read")

    def get_block(_cid):
        nonlocal reads
        reads += 1
        served.set()
        return DATA

    service = ContentService(authorize=authorize, get_block=get_block)
    client = DisseminationClient(
        identity=local,
        expected_authority=remote_authority,
        topics=(),
        authorize=authorize,
        content_service=service,
        require_dissemination=False,
    )
    running = asyncio.create_task(client.run(uri, reconnect=False))
    try:
        await asyncio.wait_for(client.connected.wait(), timeout=15)
        print(f"NETZ_MIXED_CONTENT_READY {local_authority.hex()}", flush=True)
        await asyncio.wait_for(served.wait(), timeout=15)
        if reads != 1:
            raise RuntimeError("content backend was not read exactly once")
        print("NETZ_MIXED_CONTENT_SERVED", flush=True)
    finally:
        await client.close()
        await running


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    args = parser.parse_args()
    asyncio.run(serve(args.uri, bytes.fromhex(args.remote_authority)))


if __name__ == "__main__":
    main()
