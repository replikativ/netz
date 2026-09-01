import json
import unittest
from hashlib import sha256
from importlib.util import find_spec
from pathlib import Path

from netz_codec import dumps
from netz_protocol.blob import (
    assemble,
    need_bitmap,
    needed_indices,
    split_blob,
)
from netz_protocol.replication import ReplicationError, place, rank, score
from netz_protocol.identity import authority_id, genesis
from netz_protocol.replication_wire import (
    ReplicationWireError,
    encode_frame,
    make_envelope,
    sign_statement,
    signing_bytes,
    validate_frame,
    validate_statement,
    verify_envelope,
)


ROOT = Path(__file__).resolve().parents[2]


class ReplicationPlacementTest(unittest.TestCase):
    def fixture(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-placement-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        layout = {
            "epoch": diagnostic["epoch"],
            "members": [
                {"id": member["id"],
                 "domains": {"owner": member["owner"], "site": member["site"]}}
                for member in diagnostic["members"]
            ],
        }
        return fixture, layout, diagnostic["policy"], bytes.fromhex(
            diagnostic["object_id_hex"])

    def test_shared_known_answer(self):
        fixture, layout, policy, object_id = self.fixture()
        first = layout["members"][0]["id"]
        score_input = dumps({
            "epoch": layout["epoch"], "member": first, "object": object_id,
            "profile": "netz/blob-placement/1",
        })
        self.assertEqual(fixture["first_score_input_cbor_hex"], score_input.hex())
        for member in layout["members"]:
            self.assertEqual(fixture["scores"][member["id"]],
                             score(layout["epoch"], object_id,
                                   member["id"]).hex())
        self.assertEqual(fixture["ranked_ids"],
                         [member["id"] for member in rank(layout, object_id)])
        self.assertEqual(fixture["selected_ids"],
                         [member["id"] for member in place(
                             layout, policy, object_id)])

    def test_layout_order_and_disabled_members_do_not_change_authority(self):
        _, layout, policy, object_id = self.fixture()
        expected = [member["id"] for member in place(layout, policy, object_id)]
        reversed_layout = {**layout, "members": list(reversed(layout["members"]))}
        self.assertEqual(expected, [member["id"] for member in place(
            reversed_layout, policy, object_id)])
        disabled = {**layout, "members": layout["members"] + [
            {"id": "ff" * 32, "enabled": False,
             "domains": {"owner": "mallory", "site": "elsewhere"}}
        ]}
        self.assertNotIn("ff" * 32,
                         [member["id"] for member in rank(disabled, object_id)])

    def test_policy_cannot_promise_more_domain_independence_than_w(self):
        _, layout, _, object_id = self.fixture()
        with self.assertRaises(ReplicationError):
            place(layout, {"replicas": 3, "write_acks": 1,
                           "minimum_domains": {"owner": 2}}, object_id)


class PortableBlobTest(unittest.TestCase):
    def test_shared_manifest_and_resume_fixture(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-manifest-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        split = split_blob(bytes.fromhex(diagnostic["value_hex"]),
                           diagnostic["piece_size"])
        self.assertEqual(fixture["object_id_hex"], split["object_id"].hex())
        self.assertEqual(fixture["manifest_id_hex"], split["manifest_id"].hex())
        self.assertEqual(fixture["manifest_cbor_hex"],
                         dumps(split["manifest"]).hex())
        self.assertEqual(fixture["pieces"], [
            {"id_hex": piece["id"].hex(), "data_hex": piece["data"].hex()}
            for piece in split["pieces"]
        ])
        held = {split["pieces"][index]["id"]
                for index in diagnostic["held_piece_indices"]}
        bitmap = need_bitmap(split["manifest"], held)
        self.assertEqual(fixture["need_bitmap_hex"], bitmap.hex())
        self.assertEqual(fixture["needed_piece_indices"],
                         needed_indices(split["manifest"], bitmap))
        self.assertEqual(bytes.fromhex(diagnostic["value_hex"]),
                         assemble(split["manifest"], {
                             piece["index"]: piece["data"]
                             for piece in split["pieces"]
                         }))


class SignedBlobProtocolTest(unittest.TestCase):
    def values(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-statements-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        public_key = bytes.fromhex(diagnostic["public_key_hex"])
        identity_genesis = genesis(
            [bytes.fromhex(diagnostic["rotation_key_hex"])], 1,
            [public_key], bytes.fromhex(diagnostic["revocation_commit_hex"]))
        split = split_blob(bytes.fromhex(diagnostic["value_hex"]),
                           diagnostic["piece_size"])
        statement = {
            "v": 1,
            "type": "blob/retention",
            "cluster": bytes.fromhex(diagnostic["cluster_hex"]),
            "privacy_domain": bytes.fromhex(diagnostic["privacy_domain_hex"]),
            "revision": 1,
            "manifest": split["manifest_id"],
            "object": split["object_id"],
            "layout_epoch": 7,
            "replicas": 3,
            "write_acks": 2,
            "minimum_domains": {"owner": 2, "site": 2},
            "issued_at": 1000,
            "retain_until": 2000,
            "signer": authority_id(identity_genesis),
        }
        return fixture, diagnostic, identity_genesis, public_key, split, statement

    def test_shared_statement_and_offer_fixture(self):
        values = self.values()
        fixture, diagnostic, identity_genesis, public_key, split, statement = values
        self.assertIs(statement, validate_statement(statement))
        self.assertEqual(fixture["signing_bytes_hex"], signing_bytes(statement).hex())
        envelope = make_envelope(
            identity_genesis, public_key, statement,
            bytes.fromhex(fixture["signature_hex"]))
        offer = {
            "v": 1,
            "type": "blob/offer",
            "session": bytes.fromhex(diagnostic["session_hex"]),
            "attempt": 0,
            "replica": authority_id(identity_genesis),
            "retention": envelope,
            "manifest": split["manifest"],
        }
        self.assertIs(offer, validate_frame(offer))
        self.assertEqual(fixture["offer_cbor_sha256_hex"],
                         sha256(encode_frame(offer)).hexdigest())
        fetch = {
            "v": 1,
            "type": "blob/fetch",
            "session": bytes.fromhex(diagnostic["session_hex"]),
            "attempt": 0,
            "retention": envelope,
        }
        self.assertIs(fetch, validate_frame(fetch))
        self.assertEqual(fixture["fetch_cbor_sha256_hex"],
                         sha256(encode_frame(fetch)).hexdigest())

    def test_statement_and_frame_schemas_fail_closed(self):
        values = self.values()
        fixture, diagnostic, identity_genesis, public_key, split, statement = values
        with self.assertRaises(ReplicationWireError):
            validate_statement({**statement, "extension": True})
        with self.assertRaises(ReplicationWireError):
            validate_statement({**statement, "write_acks": 4})
        envelope = make_envelope(identity_genesis, public_key, statement,
                                 bytes.fromhex(fixture["signature_hex"]))
        bad_manifest = {**split["manifest"], "object": b"\xff" * 32}
        with self.assertRaises(ReplicationWireError):
            validate_frame({
                "v": 1, "type": "blob/offer",
                "session": bytes.fromhex(diagnostic["session_hex"]),
                "attempt": 0, "replica": authority_id(identity_genesis),
                "retention": envelope, "manifest": bad_manifest,
            })

    @unittest.skipUnless(find_spec("cryptography"),
                         "optional crypto dependency is not installed")
    def test_shared_signature_and_tamper_rejection(self):
        fixture, diagnostic, identity_genesis, _, _, statement = self.values()
        signed = sign_statement(
            identity_genesis, bytes.fromhex(diagnostic["private_key_hex"]),
            statement)
        self.assertEqual(fixture["signature_hex"], signed["signature"].hex())
        self.assertTrue(verify_envelope(signed))
        tampered = {**signed, "statement": {
            **signed["statement"], "retain_until": 2001}}
        self.assertFalse(verify_envelope(tampered))


if __name__ == "__main__":
    unittest.main()
