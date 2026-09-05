"""Prototype parity and membership movement invariants."""
import unittest

from netz_protocol.replication import ReplicationError
from netz_protocol.stable_placement import place, rank, score

SEED = "a" * 64
MEMBERS = [{"id": str(i) * 64, "domains": {"site": str(i)}}
           for i in range(1, 9)]
POLICY = {"replicas": 3, "write_acks": 2}


class StablePlacementTest(unittest.TestCase):
    def targets(self, members, obj, epoch=0, policy=POLICY):
        return {m["id"] for m in place({"epoch": epoch, "members": members},
                                       SEED, policy, obj)}

    def test_epoch_order_and_membership_movement(self):
        removed = MEMBERS[0]["id"]
        for i in range(200):
            obj = f"object-{i}"
            before = self.targets(MEMBERS, obj)
            self.assertEqual(before, self.targets(list(reversed(MEMBERS)), obj, 1))
            after = self.targets(MEMBERS[1:], obj, 1)
            self.assertEqual(3, len(after))
            self.assertTrue((before - {removed}).issubset(after))
            if removed not in before:
                self.assertEqual(before, after)

    def test_domain_constraint(self):
        ranked = rank({"epoch": 0, "members": MEMBERS}, SEED, "domains")
        same_site = {m["id"] for m in ranked[:3]}
        constrained = [{**m, "domains": {"site": "same"}}
                       if m["id"] in same_site else m for m in MEMBERS]
        chosen = place({"epoch": 1, "members": constrained}, SEED,
                       {**POLICY, "minimum_domains": {"site": 2}}, "domains")
        self.assertGreaterEqual(len({m["domains"]["site"] for m in chosen}), 2)
        self.assertEqual([c * 64 for c in ["5", "6", "1"]],
                         [m["id"] for m in chosen])

    def test_disabled_member_and_invalid_seed(self):
        disabled = [{**MEMBERS[0], "enabled": False}, *MEMBERS[1:]]
        self.assertNotIn(MEMBERS[0]["id"], self.targets(disabled, "disabled"))
        with self.assertRaises(ReplicationError):
            rank({"epoch": 0, "members": MEMBERS}, "bad-seed", "x")

    def test_score_vector(self):
        self.assertEqual("93aee44dcdbfc81b052c2c17a3e396ccb76bfa2ff62b52b8ee0691269cf1ac65",
                         score(SEED, "object-0", "1" * 64).hex())


if __name__ == "__main__":
    unittest.main()
