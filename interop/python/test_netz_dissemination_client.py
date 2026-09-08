import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from netz_protocol import (
    CONTENT_CAPABILITY,
    CommitReceipt,
    ContentService,
    FileFrontierStore,
    JoinSetProfile,
    MemoryFrontierStore,
    Publisher,
    SYNC_CAPABILITY,
    SyncSnapshot,
    SyncSource,
    dumps,
    content_block_frame,
    content_want_frame,
    genesis,
    loads,
    open_frame,
    raw_cid,
)
from netz_protocol.dissemination import DISSEMINATION_CAPABILITY
from netz_protocol.dissemination_client import (
    CLIENT_CAPABILITIES,
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
    def __init__(self, capabilities=DISSEMINATION_CAPABILITIES):
        self.peer = SimpleNamespace(capabilities=capabilities)
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


class BoundJoinProfile(JoinSetProfile):
    def commit(self, value, staging, checkpoint, claimed_frontiers):
        receipt = super().commit(value, staging, checkpoint, claimed_frontiers)
        return CommitReceipt(receipt.value, claimed_frontiers,
                             len(dumps(claimed_frontiers)))


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
                frontier_store=FileFrontierStore(path, durable_ancestor=root),
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
                frontier_store=FileFrontierStore(path, durable_ancestor=root),
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

    async def test_sync_pull_runs_on_live_connection_and_commits_frontier(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        remote = bytes([16]) * 32
        contexts = []

        async def authorize(context):
            contexts.append(context)
            return True

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=remote, topics=("demo/join",),
            authorize=authorize,
        )
        session = bytes(range(16))
        profile = BoundJoinProfile()
        stream = (bytes([17]) * 32, 3, "demo/join")
        claims = [{"origin": stream[0], "epoch": stream[1],
                   "topic": stream[2], "through": 8}]
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            pulling = asyncio.create_task(client.synchronize(
                "demo/join", profile, {"python"}, session=session))
            for _ in range(100):
                if len(socket.sent) >= 3:
                    break
                await asyncio.sleep(0.01)
            capability, opened = decode_extension_frame(
                socket.sent[2], selected_capabilities=socket.peer.capabilities)
            self.assertEqual(SYNC_CAPABILITY, capability)
            self.assertEqual("sync/open", opened["type"])

            await socket.incoming.put(encode_extension_frame(
                SYNC_CAPABILITY,
                {"v": 1, "type": "sync/chunk", "session": session,
                 "index": 0, "data": dumps(["jvm"])},
                selected_capabilities=socket.peer.capabilities,
            ))
            for _ in range(100):
                if len(socket.sent) >= 4:
                    break
                await asyncio.sleep(0.01)
            _, acknowledged = decode_extension_frame(
                socket.sent[3], selected_capabilities=socket.peer.capabilities)
            self.assertEqual("sync/ack", acknowledged["type"])
            await socket.incoming.put(encode_extension_frame(
                SYNC_CAPABILITY,
                {"v": 1, "type": "sync/done", "session": session,
                 "count": 1, "topic": "demo/join",
                 "checkpoint": dumps(["jvm"]), "frontiers": claims},
                selected_capabilities=socket.peer.capabilities,
            ))
            result = await asyncio.wait_for(pulling, timeout=1)
            self.assertEqual({"jvm", "python"}, result.value)
            self.assertEqual({stream: 8}, result.frontiers)
            self.assertEqual({stream: 8}, client.committed_frontiers)
            self.assertTrue(any(
                context["op"] == "synchronize"
                and context.get("topic") == "demo/join"
                and context.get("profile") == profile.name
                for context in contexts))
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_fetches_verified_content_on_the_live_connection(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        remote = bytes([26]) * 32
        data = b"content through the reconnecting client"
        cid = raw_cid(data)
        contexts = []

        def authorize(context):
            contexts.append(context)
            return True

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=remote, topics=("db/roots",),
            authorize=authorize,
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            fetching = asyncio.create_task(client.fetch_content(cid))
            for _ in range(100):
                if len(socket.sent) >= 3:
                    break
                await asyncio.sleep(0.01)
            capability, wanted = decode_extension_frame(
                socket.sent[2], selected_capabilities=socket.peer.capabilities)
            self.assertEqual(CONTENT_CAPABILITY, capability)
            self.assertEqual("content/want", wanted["type"])
            await socket.incoming.put(encode_extension_frame(
                CONTENT_CAPABILITY,
                content_block_frame(wanted["request"], cid, data),
                selected_capabilities=socket.peer.capabilities,
            ))
            result = await asyncio.wait_for(fetching, timeout=1)
            self.assertEqual(data, result.data)
            self.assertTrue(any(
                context["op"] == "content/read"
                and context["direction"] == "inbound"
                for context in contexts))
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_content_timeout_retires_request_without_recreating_state(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([28]) * 32, topics=("db/roots",),
            authorize=lambda _context: True,
        )
        cid = raw_cid(b"late")
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            with self.assertRaisesRegex(DisseminationClientError, "timed out"):
                await client.fetch_content(cid, timeout=0.01)
            self.assertEqual({}, client.content.pending)
            self.assertEqual({}, client._content_waiters)
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_serves_content_only_after_both_policy_layers(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        remote = bytes([27]) * 32
        data = b"authorized content service"
        cid = raw_cid(data)
        events = []

        def transport_authorize(context):
            if context["frame_type"].startswith("content/"):
                events.append(("transport", context["direction"]))
            return True

        def storage_authorize(_context):
            events.append(("storage-authorize", "inbound"))
            return True

        def get_block(requested):
            events.append(("storage-read", "inbound"))
            return data if requested == cid else None

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=remote, topics=("db/roots",),
            authorize=transport_authorize,
            content_service=ContentService(
                authorize=storage_authorize, get_block=get_block),
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            await socket.incoming.put(encode_extension_frame(
                CONTENT_CAPABILITY,
                content_want_frame(bytes(range(16)), cid),
                selected_capabilities=socket.peer.capabilities,
            ))
            for _ in range(100):
                if len(socket.sent) >= 3:
                    break
                await asyncio.sleep(0.01)
            capability, response = decode_extension_frame(
                socket.sent[2], selected_capabilities=socket.peer.capabilities)
            self.assertEqual(CONTENT_CAPABILITY, capability)
            self.assertEqual("content/block", response["type"])
            self.assertEqual(
                [("transport", "inbound"),
                 ("storage-authorize", "inbound"),
                 ("storage-read", "inbound"),
                 ("transport", "outbound")],
                events,
            )
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_slow_content_backend_does_not_block_dissemination(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        remote = bytes([29]) * 32
        data = b"slow content"
        cid = raw_cid(data)
        backend_started = asyncio.Event()
        release_backend = asyncio.Event()

        async def get_block(_cid):
            backend_started.set()
            await release_backend.wait()
            return data

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=remote, topics=("db/roots",),
            authorize=lambda _context: True,
            content_service=ContentService(
                authorize=lambda _context: True, get_block=get_block),
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            await socket.incoming.put(encode_extension_frame(
                CONTENT_CAPABILITY,
                content_want_frame(bytes(range(16)), cid),
                selected_capabilities=socket.peer.capabilities,
            ))
            await asyncio.wait_for(backend_started.wait(), timeout=1)
            publication = fixture_publisher().publish("db/roots", b"root")
            await socket.incoming.put(encode_extension_frame(
                DISSEMINATION_CAPABILITY, publication,
                selected_capabilities=socket.peer.capabilities,
            ))
            delivery = await asyncio.wait_for(client.recv(), timeout=1)
            self.assertEqual(b"root", delivery.publication["payload"])
            self.assertEqual(2, len(socket.sent))
            release_backend.set()
            for _ in range(100):
                if len(socket.sent) >= 3:
                    break
                await asyncio.sleep(0.01)
            capability, response = decode_extension_frame(
                socket.sent[2], selected_capabilities=socket.peer.capabilities)
            self.assertEqual(CONTENT_CAPABILITY, capability)
            self.assertEqual("content/block", response["type"])
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_content_service_failure_closes_without_reconnect(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        cid = raw_cid(b"failed backend")

        def fail(_cid):
            raise OSError("storage unavailable")

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([30]) * 32, topics=("db/roots",),
            authorize=lambda _context: True,
            content_service=ContentService(
                authorize=lambda _context: True, get_block=fail),
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket) as opening:
            running = asyncio.create_task(client.run("wss://peer"))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            await socket.incoming.put(encode_extension_frame(
                CONTENT_CAPABILITY,
                content_want_frame(bytes(range(16)), cid),
                selected_capabilities=socket.peer.capabilities,
            ))
            with self.assertRaisesRegex(
                    DisseminationClientError, "content service request failed"):
                await asyncio.wait_for(running, timeout=1)
            self.assertEqual(1, opening.call_count)
            self.assertTrue(socket.closed)

    async def test_sync_timeout_sends_abort_and_releases_session(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([18]) * 32, topics=("demo/join",),
            authorize=lambda _context: True,
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            with self.assertRaisesRegex(DisseminationClientError, "timed out"):
                await client.synchronize(
                    "demo/join", JoinSetProfile(), set(),
                    session=bytes(range(32, 48)), timeout=0.01)
            self.assertEqual({}, client._sync_receivers)
            capability, aborted = decode_extension_frame(
                socket.sent[-1], selected_capabilities=socket.peer.capabilities)
            self.assertEqual(SYNC_CAPABILITY, capability)
            self.assertEqual("sync/abort", aborted["type"])
            self.assertEqual("timeout", aborted["code"])
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_serves_authorized_sync_source_on_live_connection(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        snapshots = []
        profile = JoinSetProfile()

        async def snapshot(context):
            snapshots.append(context)
            return SyncSnapshot({"python"})

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([19]) * 32, topics=("demo/join",),
            authorize=lambda _context: True,
            sync_sources=(SyncSource("demo/join", profile, snapshot),),
        )
        session = bytes(range(48, 64))
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            await socket.incoming.put(encode_extension_frame(
                SYNC_CAPABILITY,
                open_frame(session, "demo/join", profile, {"jvm"}),
                selected_capabilities=socket.peer.capabilities,
            ))
            for _ in range(100):
                if len(socket.sent) >= 3:
                    break
                await asyncio.sleep(0.01)
            capability, chunk = decode_extension_frame(
                socket.sent[2], selected_capabilities=socket.peer.capabilities)
            self.assertEqual(SYNC_CAPABILITY, capability)
            self.assertEqual("sync/chunk", chunk["type"])
            self.assertEqual(["python"], loads(chunk["data"]))
            await socket.incoming.put(encode_extension_frame(
                SYNC_CAPABILITY,
                {"v": 1, "type": "sync/ack", "session": session,
                 "through": 0},
                selected_capabilities=socket.peer.capabilities,
            ))
            for _ in range(100):
                if len(socket.sent) >= 4:
                    break
                await asyncio.sleep(0.01)
            _, done = decode_extension_frame(
                socket.sent[3], selected_capabilities=socket.peer.capabilities)
            self.assertEqual("sync/done", done["type"])
            served = await asyncio.wait_for(client.recv_sync_served(), timeout=1)
            self.assertEqual(session, served.session)
            self.assertEqual("synchronized", served.status)
            self.assertEqual({}, client._sync_senders)
            self.assertEqual(1, len(snapshots))
            self.assertEqual(bytes([19]) * 32,
                             snapshots[0]["peer_authority"])
            await client.close()
            await asyncio.wait_for(running, timeout=1)

    async def test_denied_sync_open_never_loads_source(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        loaded = False
        profile = JoinSetProfile()

        def snapshot(_context):
            nonlocal loaded
            loaded = True
            return SyncSnapshot(set())

        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([20]) * 32, topics=("demo/join",),
            authorize=lambda context: context["op"] != "synchronize",
            sync_sources=(SyncSource("demo/join", profile, snapshot),),
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            await socket.incoming.put(encode_extension_frame(
                SYNC_CAPABILITY,
                open_frame(bytes(range(64, 80)), "demo/join", profile, set()),
                selected_capabilities=socket.peer.capabilities,
            ))
            with self.assertRaisesRegex(DisseminationClientError,
                                        "authorization denied"):
                await asyncio.wait_for(running, timeout=1)
        self.assertFalse(loaded)

    async def test_unacknowledged_served_sync_times_out_boundedly(self):
        socket = FakeSocket(CLIENT_CAPABILITIES)
        profile = JoinSetProfile()
        session = bytes(range(80, 96))
        client = DisseminationClient(
            identity=LocalIdentity(genesis={}, operational_private=bytes(32)),
            expected_authority=bytes([21]) * 32, topics=(),
            authorize=lambda _context: True,
            sync_sources=(SyncSource(
                "demo/join", profile,
                lambda _context: SyncSnapshot({"python"})),),
            sync_timeout=0.01,
            require_dissemination=False,
        )
        with patch("netz_protocol.dissemination_client.open_secure_websocket",
                   return_value=socket):
            running = asyncio.create_task(client.run(
                "wss://peer", reconnect=False))
            await asyncio.wait_for(client.connected.wait(), timeout=1)
            await socket.incoming.put(encode_extension_frame(
                SYNC_CAPABILITY,
                open_frame(session, "demo/join", profile, set()),
                selected_capabilities=socket.peer.capabilities,
            ))
            served = await asyncio.wait_for(client.recv_sync_served(), timeout=1)
            self.assertEqual("timed-out", served.status)
            self.assertEqual({}, client._sync_senders)
            for _ in range(100):
                if len(socket.sent) >= 2:
                    _, last = decode_extension_frame(
                        socket.sent[-1],
                        selected_capabilities=socket.peer.capabilities)
                    if last["type"] == "sync/abort":
                        break
                await asyncio.sleep(0.01)
            self.assertEqual("sync/abort", last["type"])
            await client.close()
            await asyncio.wait_for(running, timeout=1)


if __name__ == "__main__":
    unittest.main()
