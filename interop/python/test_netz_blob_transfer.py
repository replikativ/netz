import json
from hashlib import sha256
from pathlib import Path
import tempfile
import unittest

from netz_protocol.blob import need_bitmap, needed_indices, split_blob
from netz_protocol.blob_store import FSYNC, FileBlobStore
from netz_protocol import (
    BlobReceiver,
    BlobSender,
    BlobTransferError,
    TransferAction,
)
from netz_protocol.identity import authority_id, genesis
from netz_protocol.replication_wire import make_envelope
from netz_protocol.replication_wire import encode_frame


SESSION = bytes.fromhex("30" * 16)
PUBLIC = bytes.fromhex(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
GENESIS = genesis([bytes.fromhex("aa" * 32)], 1, [PUBLIC],
                  bytes.fromhex("cc" * 32))
AUTHORITY = authority_id(GENESIS)
CLUSTER = bytes.fromhex("10" * 16)
PRIVACY_DOMAIN = bytes.fromhex("20" * 16)
ROOT = Path(__file__).resolve().parents[2]


def envelope(statement):
    return make_envelope(GENESIS, PUBLIC, statement, bytes(64))


def fixture(value=bytes(range(20)), piece_size=8):
    split = split_blob(value, piece_size)
    retention = {
        "v": 1,
        "type": "blob/retention",
        "cluster": CLUSTER,
        "privacy_domain": PRIVACY_DOMAIN,
        "revision": 1,
        "manifest": split["manifest_id"],
        "object": split["object_id"],
        "layout_epoch": 7,
        "replicas": 1,
        "write_acks": 1,
        "placement_minimum_domains": {}, "ack_minimum_domains": {},
        "issued_at": 1000,
        "retain_until": 5000,
        "signer": AUTHORITY,
    }
    offer = {
        "v": 1,
        "type": "blob/offer",
        "session": SESSION,
        "attempt": 0,
        "replica": AUTHORITY,
        "retention": envelope(retention),
        "manifest": split["manifest"],
    }
    return split, retention, offer


def one(actions, kind):
    matches = [action for action in actions if action.kind == kind]
    if len(matches) != 1:
        raise AssertionError(f"expected one {kind} action, got {actions!r}")
    return matches[0].values


class PythonBlobTransferTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix="netz-python-transfer-")
        self.store = FileBlobStore(self.temporary.name, durable_ancestor=self.temporary.name,
                                   clock=lambda: 1200)

    async def asyncTearDown(self):
        await self.store.close()
        self.temporary.cleanup()

    async def test_file_store_round_trip_is_gated_by_every_durable_boundary(self):
        split, _, offer = fixture()
        sender = BlobSender("receiver", AUTHORITY, offer, now=1000)
        receiver = BlobReceiver(
            AUTHORITY, "sender", AUTHORITY, offer, now=1000)

        started = sender.start()
        self.assertIsInstance(started[0], TransferAction)
        sent_offer = one(started, "send")["frame"]
        self.assertEqual(offer, sent_offer)
        load = one(receiver.start(), "load_staging")
        staging = await self.store.load_staging(
            load["session"], load["manifest_id"], load["manifest"])
        need_frame = one(receiver.staging_loaded(**staging, now=1000),
                         "send")["frame"]

        actions = sender.receive("receiver", AUTHORITY, need_frame)
        while sender.status == "loading-piece":
            load_piece = one(actions, "load_piece")
            piece = split["pieces"][load_piece["index"]]
            actions = sender.piece_loaded(
                piece["index"], "ok", piece["data"])
            piece_frame = one(actions, "send")["frame"]

            stage = one(receiver.receive(
                "sender", AUTHORITY, piece_frame), "stage_piece")
            # No ACK exists until the repository confirms staging.
            result = await self.store.stage_piece(
                stage["session"], stage["manifest_id"], stage["manifest"],
                stage["index"], stage["piece_id"], stage["data"])
            ack = one(receiver.piece_staged(stage["index"], result),
                      "send")["frame"]
            actions = sender.receive("receiver", AUTHORITY, ack)

        commit = one(actions, "send")["frame"]
        self.assertEqual("blob/commit", commit["type"])
        install = one(receiver.receive("sender", AUTHORITY, commit),
                      "install_object")
        installed = await self.store.install_object(
            install["session"], install["manifest_id"], install["manifest"],
            install["retention"], install["persistence"])
        sign = one(receiver.installed(
            installed["result"], installed["persistence"],
            installed["stored_at"], now=1200), "sign_receipt")
        receiver_done = receiver.receipt_signed(envelope(sign["statement"]))
        stored = one(receiver_done, "send")["frame"]

        persist = sender.verified_stored("receiver", AUTHORITY, stored)
        self.assertEqual("persist_receipt", persist[0].kind)
        self.assertNotEqual("complete", sender.status)
        controller = one(sender.receipt_persisted("ok"),
                         "replication_stored")["event"]

        self.assertEqual("complete", sender.status)
        self.assertEqual("complete", receiver.status)
        self.assertEqual(split["object_id"].hex(), controller["object_id"])
        self.assertEqual(FSYNC, controller["persistence"])
        self.assertFalse(hasattr(sender, "piece_data"))
        self.assertEqual(
            bytes(range(20)),
            await self.store.read_installed(split["manifest_id"], 20),
        )

    async def test_jvm_javascript_python_transfer_trace_bytes_match(self):
        split, _, _ = fixture()
        expected = json.loads((
            ROOT / "spec/fixtures/blob-statements-1.json"
        ).read_text())["transfer_trace"]
        def digest(frame):
            return sha256(encode_frame(frame)).hexdigest()
        need = {
            "v": 1, "type": "blob/need", "session": SESSION,
            "manifest": split["manifest_id"],
            "need": need_bitmap(split["manifest"], set()),
        }
        pieces = [{
            "v": 1, "type": "blob/piece", "session": SESSION,
            "manifest": split["manifest_id"], "index": piece["index"],
            "data": piece["data"],
        } for piece in split["pieces"]]
        acks = [{
            "v": 1, "type": "blob/ack", "session": SESSION,
            "manifest": split["manifest_id"], "through": piece["index"],
        } for piece in split["pieces"]]
        commit = {
            "v": 1, "type": "blob/commit", "session": SESSION,
            "manifest": split["manifest_id"], "count": len(split["pieces"]),
        }
        self.assertEqual(expected["need_cbor_sha256_hex"], digest(need))
        self.assertEqual(expected["piece_cbor_sha256_hexes"],
                         [digest(frame) for frame in pieces])
        self.assertEqual(expected["ack_cbor_sha256_hexes"],
                         [digest(frame) for frame in acks])
        self.assertEqual(expected["commit_cbor_sha256_hex"], digest(commit))

    async def test_receiver_enforces_one_piece_storage_backpressure(self):
        split, _, offer = fixture()
        receiver = BlobReceiver(
            AUTHORITY, "sender", AUTHORITY, offer, now=1000)
        receiver.start()
        receiver.staging_loaded([], now=1000)
        piece0, piece1 = split["pieces"][:2]

        first = receiver.receive("sender", AUTHORITY, {
            "v": 1, "type": "blob/piece", "session": SESSION,
            "manifest": split["manifest_id"], "index": 0,
            "data": piece0["data"],
        })
        self.assertEqual(["stage_piece"], [action.kind for action in first])
        second = receiver.receive("sender", AUTHORITY, {
            "v": 1, "type": "blob/piece", "session": SESSION,
            "manifest": split["manifest_id"], "index": 1,
            "data": piece1["data"],
        })
        self.assertEqual("aborted", receiver.status)
        self.assertEqual("piece-before-stage", receiver.abort_code)
        self.assertEqual(["send", "cleanup_staging"],
                         [action.kind for action in second])

    async def test_resume_and_repeated_piece_addresses_are_exact(self):
        split, _, offer = fixture(bytes([1, 2, 1, 2]), 2)
        receiver = BlobReceiver(
            AUTHORITY, "sender", AUTHORITY, offer, now=1000)
        receiver.start()
        need = one(receiver.staging_loaded(
            [split["pieces"][0]["id"]], now=1000), "send")["frame"]
        self.assertEqual([], needed_indices(split["manifest"], need["need"]))

    async def test_receipt_mismatch_and_failed_persistence_never_complete(self):
        split, retention, offer = fixture(b"", 8)
        sender = BlobSender("receiver", AUTHORITY, offer, now=1000)
        sender.start()
        all_held = {
            "v": 1, "type": "blob/need", "session": SESSION,
            "manifest": split["manifest_id"],
            "need": need_bitmap(split["manifest"], set()),
        }
        commit = one(sender.receive("receiver", AUTHORITY, all_held),
                     "send")["frame"]
        self.assertEqual("blob/commit", commit["type"])
        receipt = {
            "v": 1, "type": "blob/stored",
            "cluster": CLUSTER, "privacy_domain": PRIVACY_DOMAIN,
            "retention_revision": 1, "manifest": split["manifest_id"],
            "object": split["object_id"], "layout_epoch": 7,
            "replica": AUTHORITY, "attempt": 1,
            "persistence": FSYNC, "stored_at": 1200,
            "retain_until": retention["retain_until"], "signer": AUTHORITY,
        }
        rejected = sender.verified_stored("receiver", AUTHORITY, {
            "v": 1, "type": "blob/stored", "session": SESSION,
            "receipt": envelope(receipt),
        })
        self.assertEqual("aborted", sender.status)
        self.assertEqual("receipt-mismatch", sender.abort_code)
        self.assertNotIn("replication_stored",
                         [action.kind for action in rejected])

        sender = BlobSender("receiver", AUTHORITY, offer, now=1000)
        sender.start()
        sender.receive("receiver", AUTHORITY, all_held)
        receipt["attempt"] = 0
        direct = BlobSender("receiver", AUTHORITY, offer, now=1000)
        direct.start()
        direct_actions = direct.verified_stored("receiver", AUTHORITY, {
            "v": 1, "type": "blob/stored", "session": SESSION,
            "receipt": envelope(receipt),
        })
        self.assertEqual("persisting-receipt", direct.status)
        self.assertEqual(["persist_receipt"], [a.kind for a in direct_actions])
        sender.verified_stored("receiver", AUTHORITY, {
            "v": 1, "type": "blob/stored", "session": SESSION,
            "receipt": envelope(receipt),
        })
        failed = sender.receipt_persisted("disk-full")
        self.assertEqual("aborted", sender.status)
        self.assertEqual(["transfer_failed"],
                         [action.kind for action in failed])

    async def test_options_and_authenticated_connection_fail_closed(self):
        _, _, offer = fixture()
        with self.assertRaises(BlobTransferError):
            BlobSender("receiver", bytes(32), offer, now=1000)
        with self.assertRaises(BlobTransferError):
            BlobSender("receiver", AUTHORITY, offer, now=True)

        sender = BlobSender("receiver", AUTHORITY, offer, now=1000)
        receiver = BlobReceiver(
            AUTHORITY, "sender", AUTHORITY, offer, now=1000)
        offer["attempt"] = 99
        self.assertEqual(0, one(sender.start(), "send")["frame"]["attempt"])
        load = one(receiver.start(), "load_staging")
        load["manifest"]["length"] = 99
        self.assertEqual(20, receiver.manifest["length"])

        receiver = BlobReceiver(
            AUTHORITY, "sender", AUTHORITY, offer, now=1000)
        receiver.start()
        receiver.staging_loaded([], now=1000)
        actions = receiver.receive("other", AUTHORITY, {
            "v": 1, "type": "blob/abort", "session": SESSION,
            "code": "stop",
        })
        self.assertEqual("aborted", receiver.status)
        self.assertEqual("connection-or-session-mismatch", receiver.abort_code)
        self.assertEqual(["send", "cleanup_staging"],
                         [action.kind for action in actions])


if __name__ == "__main__":
    unittest.main()
