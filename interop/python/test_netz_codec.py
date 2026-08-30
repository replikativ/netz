import json
import unittest
from pathlib import Path

from netz_codec import CBORError, dumps, loads
from netz_sync import JoinSetProfile, RootLogProfile, SyncError, SyncReceiver, open_frame
from netz_protocol import dumps as package_dumps


ROOT = Path(__file__).resolve().parents[2]


class CodecTest(unittest.TestCase):
    def test_installed_package_facade(self):
        self.assertEqual(dumps({"v": 1}), package_dumps({"v": 1}))

    def fixture(self):
        fixture = json.loads((ROOT / "spec/fixtures/profile-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        value = {
            "v": diagnostic["v"],
            "type": diagnostic["type"],
            "peer": diagnostic["peer"],
            "nonce": bytes.fromhex(diagnostic["nonce_hex"]),
            "features": diagnostic["features"],
        }
        return fixture, value

    def test_shared_known_answer(self):
        fixture, value = self.fixture()
        encoded = dumps(value)
        self.assertEqual(fixture["canonical_cbor_hex"], encoded.hex())
        self.assertEqual(value, loads(encoded))

    def test_shared_sync_open_known_answer(self):
        fixture = json.loads((ROOT / "spec/fixtures/sync-open-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        profile = JoinSetProfile()
        value = set(diagnostic["summary_diagnostic"])
        frame = open_frame(bytes.fromhex(diagnostic["session_hex"]),
                           diagnostic["topic"], profile, value)
        self.assertEqual(diagnostic["summary_cbor_hex"], frame["summary"].hex())
        self.assertEqual(fixture["canonical_cbor_hex"], dumps(frame).hex())
        self.assertEqual(frame, loads(dumps(frame)))

    def test_shared_sync_done_known_answer(self):
        fixture = json.loads((ROOT / "spec/fixtures/sync-done-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        frame = {
            "v": diagnostic["v"],
            "type": diagnostic["type"],
            "session": bytes.fromhex(diagnostic["session_hex"]),
            "count": diagnostic["count"],
            "topic": diagnostic["topic"],
            "checkpoint": bytes.fromhex(diagnostic["checkpoint_hex"]),
            "frontiers": [
                {"origin": bytes.fromhex(item["origin_hex"]),
                 "epoch": item["epoch"], "topic": item["topic"],
                 "through": item["through"]}
                for item in diagnostic["frontiers"]
            ],
        }
        self.assertEqual(fixture["canonical_cbor_hex"], dumps(frame).hex())
        self.assertEqual(frame, loads(dumps(frame)))

    def test_rejects_non_profile_values(self):
        with self.assertRaises(CBORError):
            dumps({1: "non-text key"})
        with self.assertRaises(CBORError):
            dumps(1.5)
        with self.assertRaises(CBORError):
            dumps(1 << 63)
        with self.assertRaises(CBORError):
            loads(b"\x18\x01")  # non-minimal encoding of 1
        with self.assertRaises(CBORError):
            loads(b"\x01\x02")  # trailing item
        with self.assertRaises(CBORError):
            loads(b"\x7a\x00\x10\x00\x01")  # text longer than 1 MiB
        with self.assertRaises(CBORError):
            loads(b"\x99\x40\x01")  # array claims 16,385 items

    def test_sync_receiver_is_ordered_and_atomic(self):
        session = bytes(range(16))
        profile = JoinSetProfile()
        receiver = SyncReceiver(session, "demo/tasks", profile, {"local"})
        ack = receiver.receive({"v": 1, "type": "sync/chunk", "session": session,
                                "index": 0, "data": dumps(["remote"])})
        self.assertEqual(0, ack["through"])
        self.assertEqual({"local"}, receiver.value, "staging must not leak before done")
        receiver.receive({"v": 1, "type": "sync/done", "session": session,
                          "count": 1, "topic": "demo/tasks",
                          "frontiers": [{"origin": bytes(32), "epoch": 0,
                                         "topic": "demo/tasks", "through": 999}],
                          "checkpoint": dumps(["remote"])})
        self.assertEqual({"local", "remote"}, receiver.value)
        self.assertEqual({}, receiver.frontiers,
                         "legacy profile must not echo the sender's frontier")
        self.assertEqual("synchronized", receiver.status)

        future = SyncReceiver(session, "demo/tasks", profile, set())
        with self.assertRaises(SyncError):
            future.receive({"v": 1, "type": "sync/chunk", "session": session,
                            "index": 1, "data": dumps(["future"])})

    def test_sync_receiver_rejects_frontiers_before_profile_commit(self):
        class CountingProfile(JoinSetProfile):
            def __init__(self):
                self.commits = 0

            def commit(self, value, staging, checkpoint, claimed_frontiers):
                self.commits += 1
                return super().commit(value, staging, checkpoint,
                                      claimed_frontiers)

        session = bytes(range(16))
        profile = CountingProfile()
        receiver = SyncReceiver(session, "demo/tasks", profile, set())
        duplicate = {"origin": bytes(32), "epoch": 0,
                     "topic": "demo/tasks", "through": 0}
        with self.assertRaises(SyncError):
            receiver.receive({"v": 1, "type": "sync/done", "session": session,
                              "count": 0, "topic": "demo/tasks",
                              "frontiers": [duplicate, duplicate],
                              "checkpoint": dumps([])})
        self.assertEqual(0, profile.commits)

    def test_root_profile_rejects_wrong_writer_and_conflict(self):
        profile = RootLogProfile("urd/a")
        current = {"writer": "urd/a", "term": 2, "index": 7, "root": "cid-a"}
        with self.assertRaises(SyncError):
            profile.apply_chunk(None, dumps({**current, "writer": "unselected"}))
        with self.assertRaises(SyncError):
            profile.apply_chunk(current, dumps({**current, "root": "cid-b"}))


if __name__ == "__main__":
    unittest.main()
