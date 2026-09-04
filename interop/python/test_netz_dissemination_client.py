import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from netz_protocol import FileFrontierStore, MemoryFrontierStore, Publisher, genesis
from netz_protocol.dissemination import DISSEMINATION_CAPABILITY
from netz_protocol.dissemination_client import (
    DISSEMINATION_CAPABILITIES,
    DisseminationClient,
    DisseminationClientError,
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


class FailingFrontierStore:
    async def load_frontiers(self):
        return {}

    async def save_frontiers(self, _frontiers):
        raise OSError("disk unavailable")


class CorruptFrontierStore:
    async def load_frontiers(self):
        raise ValueError("corrupt")

    async def save_frontiers(self, _frontiers):
        raise AssertionError("unreachable")


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
            self.assertEqual(b"root", delivered.publication["payload"])
            await delivered.commit()
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
            delivery = await asyncio.wait_for(client.recv(), timeout=1)
            await delivery.commit()
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

    async def test_committed_frontier_restores_after_process_restart(self):
        remote = bytes([11]) * 32
        publication = fixture_publisher().publish("db/roots", b"durable")
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "frontiers.cbor"
            first = FakeSocket()
            client = DisseminationClient(
                identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
                expected_authority=remote, topics=("db/roots",),
                authorize=lambda _context: True,
                frontier_store=FileFrontierStore(path),
            )
            with patch("netz_protocol.dissemination_client.open_secure_websocket",
                       return_value=first):
                running = asyncio.create_task(client.run(
                    "wss://peer", reconnect=False))
                await asyncio.wait_for(client.connected.wait(), timeout=1)
                await first.incoming.put(encode_extension_frame(
                    DISSEMINATION_CAPABILITY, publication,
                    selected_capabilities=first.peer.capabilities,
                ))
                delivery = await asyncio.wait_for(client.recv(), timeout=1)
                self.assertFalse(path.exists(),
                                 "network receipt must not commit application state")
                await delivery.commit()
                self.assertTrue(path.exists())
                await client.close()
                await asyncio.wait_for(running, timeout=1)

            second = FakeSocket()
            restored = DisseminationClient(
                identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
                expected_authority=remote, topics=("db/roots",),
                authorize=lambda _context: True,
                frontier_store=FileFrontierStore(path),
            )
            with patch("netz_protocol.dissemination_client.open_secure_websocket",
                       return_value=second):
                running = asyncio.create_task(restored.run(
                    "wss://peer", reconnect=False))
                await asyncio.wait_for(restored.connected.wait(), timeout=1)
                selected = second.peer.capabilities
                announced = [decode_extension_frame(
                    value, selected_capabilities=selected)[1]
                    for value in second.sent[:2]]
                self.assertEqual([[0, 0]],
                                 announced[1]["streams"][0]["ranges"])
                await second.incoming.put(encode_extension_frame(
                    DISSEMINATION_CAPABILITY, publication,
                    selected_capabilities=selected,
                ))
                await asyncio.sleep(0.02)
                self.assertTrue(restored.publications.empty())
                await restored.close()
                await asyncio.wait_for(running, timeout=1)

    async def test_commit_is_contiguous_and_captures_verified_sequence(self):
        publisher = fixture_publisher()
        first = publisher.publish("db/roots", b"first")
        second = publisher.publish("db/roots", b"second")
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([12]) * 32, topics=("db/roots",),
            authorize=lambda _context: True,
            frontier_store=MemoryFrontierStore(),
        )
        await client._restore()
        first_delivery = client._delivery(first)
        second_delivery = client._delivery(second)
        with self.assertRaisesRegex(DisseminationClientError, "contiguous"):
            await second_delivery.commit()
        first_delivery.publication["seq"] = 99
        await first_delivery.commit()
        await second_delivery.commit()
        stream = client.session.receiver.stream(first)
        self.assertEqual({stream: 1}, client.committed_frontiers)

    async def test_failed_store_never_advances_memory_frontier(self):
        publication = fixture_publisher().publish("db/roots", b"root")
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([13]) * 32, topics=("db/roots",),
            authorize=lambda _context: True,
            frontier_store=FailingFrontierStore(),
        )
        await client._restore()
        with self.assertRaisesRegex(DisseminationClientError, "not persisted"):
            await client._delivery(publication).commit()
        self.assertEqual({}, client.committed_frontiers)

    async def test_corrupt_frontier_fails_before_network_disclosure(self):
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([15]) * 32, topics=("db/roots",),
            authorize=lambda _context: True,
            frontier_store=CorruptFrontierStore(),
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket") \
                as opening:
            with self.assertRaisesRegex(DisseminationClientError,
                                        "could not be restored"):
                await client.run("wss://peer", reconnect=False)
        opening.assert_not_called()

    async def test_state_sync_commit_persists_then_skips_covered_queue(self):
        publisher = fixture_publisher()
        first = publisher.publish("db/roots", b"first")
        second = publisher.publish("db/roots", b"second")
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([14]) * 32, topics=("db/roots",),
            authorize=lambda _context: True,
        )
        await client._restore()
        stream = client.session.receiver.stream(first)
        await client.publications.put(client._delivery(first))
        await client.commit_frontiers({stream: 0})
        await client.publications.put(client._delivery(second))
        delivery = await asyncio.wait_for(client.recv(), timeout=1)
        self.assertEqual(1, delivery.publication["seq"])
        await delivery.commit()
        with self.assertRaisesRegex(DisseminationClientError, "could not be committed"):
            await client.commit_frontiers({stream: 0})


if __name__ == "__main__":
    unittest.main()
