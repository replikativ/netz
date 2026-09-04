import json
import unittest
from pathlib import Path

from netz_protocol import Publisher, genesis
from netz_protocol.dissemination_session import DisseminationSession


ROOT = Path(__file__).resolve().parents[2]


def publisher(epoch=7):
    fixture = json.loads(
        (ROOT / "spec/fixtures/publication-1.json").read_text())
    value = fixture["diagnostic"]
    identity = genesis(
        [bytes.fromhex(value["rotation_public_hex"])], 1,
        [bytes.fromhex(value["operational_public_hex"])],
        bytes.fromhex(value["revoke_hex"]),
    )
    return Publisher(identity,
                     bytes.fromhex(value["operational_private_hex"]),
                     epoch=epoch)


class DisseminationSessionTest(unittest.TestCase):
    def test_interest_publication_gap_and_repair(self):
        sender = DisseminationSession(topics=(), publisher=publisher())
        receiver = DisseminationSession(topics=("db/roots",))
        interests, empty_have = receiver.connected()
        sender.receive(interests)
        self.assertEqual([], empty_have["streams"])

        first = sender.publish("db/roots", b"root-0")
        second = sender.publish("db/roots", b"root-1")
        third = sender.publish("db/roots", b"root-2")
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertIsNotNone(third)

        initial = receiver.receive(first)
        self.assertEqual((first,), initial.delivered)
        gap = receiver.receive(third)
        self.assertEqual(1, len(gap.outbound))
        self.assertEqual(1, gap.outbound[0]["ranges"][0]["lo"])
        repair = sender.receive(gap.outbound[0])
        self.assertEqual(1, len(repair.outbound))
        repaired = receiver.receive(repair.outbound[0])
        self.assertEqual([1, 2], [item["seq"] for item in repaired.delivered])

        _, reconnect_have = receiver.connected()
        self.assertEqual([[0, 2]], reconnect_have["streams"][0]["ranges"])

    def test_remote_interests_gate_new_publications_but_not_history(self):
        sender = DisseminationSession(topics=(), publisher=publisher())
        sender.receive({"v": 1, "type": "interests", "topics": [],
                        "carries": [], "relay_all": False})
        self.assertFalse(sender.peer_interested("db/roots"))
        sender.publish("db/roots", b"not-live")
        self.assertEqual(1, len(sender.have()["streams"]))

    def test_have_requests_are_bounded_without_expanding_large_ranges(self):
        receiver = DisseminationSession(topics=("db/roots",), max_want_span=8)
        origin = bytes(range(32))
        result = receiver.receive({
            "v": 1,
            "type": "have",
            "streams": [{"origin": origin, "epoch": 1, "topic": "db/roots",
                         "ranges": [[0, 9_007_199_254_740_991]], "floor": 0}],
        })
        ranges = result.outbound[0]["ranges"]
        self.assertEqual(16, len(ranges))
        self.assertTrue(all(value["hi"] - value["lo"] < 8 for value in ranges))
        self.assertEqual({"lo": 0, "hi": 7},
                         {key: ranges[0][key] for key in ("lo", "hi")})

    def test_horizon_reports_state_sync_and_requests_repairable_suffix(self):
        receiver = DisseminationSession(topics=("db/roots",))
        origin = bytes(range(32))
        result = receiver.receive({
            "v": 1,
            "type": "have",
            "streams": [{"origin": origin, "epoch": 1, "topic": "db/roots",
                         "ranges": [[9, 12]], "floor": 9}],
        })
        self.assertEqual(((origin, 1, "db/roots"), 9, 12),
                         result.state_sync_required[0])
        self.assertEqual({"lo": 9, "hi": 12},
                         {key: result.outbound[0]["ranges"][0][key]
                          for key in ("lo", "hi")})


if __name__ == "__main__":
    unittest.main()
