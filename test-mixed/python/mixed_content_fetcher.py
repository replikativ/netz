#!/usr/bin/env python3
"""Fetch one portable raw-CID block from the JVM service."""

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    CONTENT_CAPABILITY,
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    DisseminationClient,
    genesis,
    raw_cid,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity

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
    cid = raw_cid(DATA)
    client = DisseminationClient(
        identity=local,
        expected_authority=remote_authority,
        topics=(),
        authorize=lambda context: (
            context["peer_authority"] == remote_authority
            and context["op"] == "content/read"),
        content_timeout_ms=15_000,
        require_dissemination=False,
    )
    running = asyncio.create_task(client.run(uri, reconnect=False))
    try:
        await asyncio.wait_for(client.connected.wait(), timeout=15)
        result = await client.fetch_content(cid, timeout=15)
        if result.data != DATA:
            raise RuntimeError("JVM returned the wrong content block")
    finally:
        await client.close()
        await running


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
