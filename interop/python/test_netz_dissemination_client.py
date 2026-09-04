import asyncio
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from netz_protocol import Publisher, genesis
from netz_protocol.dissemination import DISSEMINATION_CAPABILITY
from netz_protocol.dissemination_client import (
    DISSEMINATION_CAPABILITIES,
    DisseminationClient,
)
from netz_protocol.extension import decode_extension_frame, encode_extension_frame
from netz_protocol.secure import LocalIdentity


ROOT = Path(__file__).resolve().parents[2]


def fixture_publisher():
    fixture = json.loads(
        (ROOT / "spec/fixtures/publication-1.json").read_text())["diagnostic"]
    value = genesis(
        [bytes.fromhex(fixture["rotation_public_hex"])], 1,
        [bytes.fromhex(fixture["operational_public_hex"])],
        bytes.fromhex(fixture["revoke_hex"]),
    )
    return Publisher(value, bytes.fromhex(fixture["operational_private_hex"]),
                     epoch=4)


class FakeSocket:
    def __init__(self):
        self.peer = SimpleNamespace(capabilities=DISSEMINATION_CAPABILITIES)
        self.websocket = self
        self.sent = []
        self.incoming = asyncio.Queue()
        self.closed = False

    async def send_frame(self, value):
        self.sent.append(value)

    async def recv_frame(self):
        value = await self.incoming.get()
        if isinstance(value, BaseException):
            raise value
        return value

    async def close(self):
        if not self.closed:
            self.closed = True
            await self.incoming.put(ConnectionError("closed"))


class DisseminationClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_publisher_requires_an_explicit_restart_epoch(self):
        with self.assertRaisesRegex(ValueError, "explicitly managed epoch"):
            DisseminationClient(
                identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
                expected_authority=bytes([7]) * 32,
                topics=("db/roots",), authorize=lambda _context: True,
                publish=True,
            )

    async def test_connects_announces_and_delivers_verified_publication(self):
        socket = FakeSocket()
        remote = bytes([8]) * 32
        identity = LocalIdentity(genesis={}, operational_private=bytes(32))
        contexts = []

        async def authorize(context):
            contexts.append(context)
            return context["peer_authority"] == remote

        client = DisseminationClient(
            identity=identity, expected_authority=remote,
            topics=("db/roots",), authorize=authorize,
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run("wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            self.assertEqual(2, len(socket.sent))
            selected = socket.peer.capabilities
            announced = [decode_extension_frame(value,
                                                  selected_capabilities=selected)
                         for value in socket.sent]
            self.assertEqual([DISSEMINATION_CAPABILITY] * 2,
                             [value[0] for value in announced])
            self.assertEqual(["interests", "have"],
                             [value[1]["type"] for value in announced])

            publication = fixture_publisher().publish("db/roots", b"root")
            await socket.incoming.put(encode_extension_frame(
                DISSEMINATION_CAPABILITY, publication,
                selected_capabilities=selected,
            ))
            delivered = await asyncio.wait_for(client.recv(), timeout=1)
            self.assertEqual(b"root", delivered["payload"])
            self.assertTrue(any(value["direction"] == "inbound"
                                and value.get("origin_authority") == publication["origin"]
                                for value in contexts))
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_policy_denial_fails_non_reconnecting_client_closed(self):
        socket = FakeSocket()
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([9]) * 32,
            topics=("db/roots",), authorize=lambda _context: False,
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            with self.assertRaises(ConnectionError):
                await client.run("wss://peer", reconnect=False)
        self.assertTrue(socket.closed)

    async def test_reconnect_retains_frontier_and_reannounces_have(self):
        first, second = FakeSocket(), FakeSocket()
        remote = bytes([10]) * 32
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=remote, topics=("db/roots",),
            authorize=lambda _context: True, have_interval=60,
        )
        sockets = iter((first, second))

        async def connect(*_args, **_kwargs):
            return next(sockets)

        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   side_effect=connect) as opening:
            running = asyncio.create_task(client.run(
                "wss://peer", initial_backoff=0.01, max_backoff=0.02))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            publication = fixture_publisher().publish("db/roots", b"root")
            await first.incoming.put(encode_extension_frame(
                DISSEMINATION_CAPABILITY, publication,
                selected_capabilities=first.peer.capabilities,
            ))
            await asyncio.wait_for(client.recv(), timeout=1)
            await first.incoming.put(ConnectionError("partition"))
            for _ in range(100):
                if opening.call_count >= 2 and len(second.sent) >= 2:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(2, opening.call_count)
            announced = [decode_extension_frame(
                value, selected_capabilities=second.peer.capabilities)[1]
                for value in second.sent[:2]]
            self.assertEqual([[0, 0]], announced[1]["streams"][0]["ranges"])
            await client.close()
            await asyncio.wait_for(running, timeout=1)


if __name__ == "__main__":
    unittest.main()
