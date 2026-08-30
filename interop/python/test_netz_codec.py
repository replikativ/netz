import json
import unittest
from pathlib import Path

from netz_codec import CBORError, dumps, loads


ROOT = Path(__file__).resolve().parents[2]


class CodecTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
