#!/usr/bin/env python3
"""Serve one portable join-semilattice synchronization to the JVM."""

from __future__ import annotations

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import (
    DisseminationClient,
    EXTENSION_CARRIER_CAPABILITY,
    WIRE_PROFILE_CAPABILITY,
    JoinSetProfile,
    SYNC_CAPABILITY,
    SyncSnapshot,
    SyncSource,
    genesis,
)
from netz_protocol.crypto import DEFAULT_CAPABILITIES
from netz_protocol.secure import LocalIdentity

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
    profile = JoinSetProfile()
    client = DisseminationClient(
        identity=_identity(),
        expected_authority=remote_authority,
        topics=(),
        authorize=lambda context: (
            context["peer_authority"] == remote_authority
            and (context["op"] != "synchronize"
                 or (context.get("topic") == TOPIC
                     and context.get("profile") == profile.name))
        ),
        sync_sources=(SyncSource(
            TOPIC, profile,
            lambda _context: SyncSnapshot({"python"})),),
        sync_timeout=15,
        require_dissemination=False,
    )
    running = asyncio.create_task(client.run(uri, reconnect=False))
    try:
        await asyncio.wait_for(client.connected.wait(), timeout=15)
        served = await asyncio.wait_for(client.recv_sync_served(), timeout=15)
        if served.status != "synchronized":
            raise RuntimeError(f"synchronization ended as {served.status}")
    finally:
        await client.close()
        await running


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
