#!/usr/bin/env python3
"""Receive an offline JVM publication through reusable Python repair."""

from __future__ import annotations

import argparse
import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol import genesis
from netz_protocol.dissemination_client import DisseminationClient
from netz_protocol.secure import LocalIdentity

TOPIC = "datahike/roots/python-demo"


def _identity() -> LocalIdentity:
    private = bytes([19]) * 32
    public = (ed25519.Ed25519PrivateKey.from_private_bytes(private)
              .public_key().public_bytes_raw())
    return LocalIdentity(
        genesis=genesis([public], 1, [public], bytes([51]) * 32),
        operational_private=private,
    )


async def receive(uri: str, remote_authority: bytes) -> bytes:
    def authorize(context):
        if context["peer_authority"] != remote_authority:
            return False
        if context["frame_type"] != "gossip":
            return True
        return (context["origin_authority"] == remote_authority
                and context["topic"] == TOPIC)

    client = DisseminationClient(
        identity=_identity(), expected_authority=remote_authority,
        topics=(TOPIC,), authorize=authorize,
    )
    running = asyncio.create_task(client.run(uri, reconnect=False))
    try:
        delivery = await asyncio.wait_for(client.recv(), timeout=20)
        # A real application writes the root before this call. The conformance
        # client has no application store, so retaining the bytes is its commit.
        payload = delivery.publication["payload"]
        await delivery.commit()
        return payload
    finally:
        await client.close()
        await running


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--remote-authority", required=True)
    args = parser.parse_args()
    authority = bytes.fromhex(args.remote_authority)
    if len(authority) != 32:
        parser.error("--remote-authority must be 32 bytes of hexadecimal")
    payload = asyncio.run(receive(args.uri, authority))
    print(f"NETZ_MIXED_DISSEMINATION_OK {payload.decode('utf-8')}", flush=True)


if __name__ == "__main__":
    main()
