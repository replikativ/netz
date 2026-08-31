import json
import unittest
from pathlib import Path

from netz_codec import dumps
from netz_protocol.blob import (
    assemble,
    need_bitmap,
    needed_indices,
    split_blob,
)
from netz_protocol.replication import ReplicationError, place, rank, score


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


if __name__ == "__main__":
    unittest.main()
