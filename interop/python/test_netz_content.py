import json
import unittest
from pathlib import Path

from netz_protocol.content import (
    MAX_BLOCK_BYTES,
    ContentError,
    announce_frame,
    block_frame,
    decode_frame,
    dont_have_frame,
    encode_frame,
    find_frame,
    found_frame,
    raw_cid,
    validate_frame,
    verify_block,
    want_frame,
)


FIXTURE = json.loads(
    (Path(__file__).parents[2] / "spec/fixtures/content-block-1.json").read_text()
)
DATA = FIXTURE["data_utf8"].encode()
CID = bytes.fromhex(FIXTURE["cid_hex"])
REQUEST = bytes.fromhex(FIXTURE["request_hex"])
PROVIDER = bytes.fromhex(FIXTURE["provider_hex"])


class ContentTest(unittest.TestCase):
    def test_shared_profile_vectors(self):
        frames = {
            "announce": announce_frame([CID]),
            "find": find_frame(REQUEST, CID),
            "found": found_frame(REQUEST, CID, [PROVIDER], ttl_ms=60_000),
            "want": want_frame(REQUEST, CID),
            "block": block_frame(REQUEST, CID, DATA),
            "dont_have": dont_have_frame(REQUEST, CID),
        }
        self.assertEqual(CID, raw_cid(DATA))
        for name, frame in frames.items():
            encoded = encode_frame(frame)
            self.assertEqual(FIXTURE[f"{name}_cbor_hex"], encoded.hex())
            self.assertEqual(frame, decode_frame(encoded))

    def test_integrity_and_bounds_are_enforced(self):
        self.assertTrue(verify_block(CID, DATA))
        self.assertFalse(verify_block(CID, DATA + b"!"))
        with self.assertRaises(ContentError):
            block_frame(REQUEST, CID, DATA + b"!")
        with self.assertRaises(ContentError):
            raw_cid(b"x" * (MAX_BLOCK_BYTES + 1))
        with self.assertRaises(ContentError):
            find_frame(REQUEST[:-1], CID)

    def test_decoded_frames_are_closed_and_canonical(self):
        frame = announce_frame([CID])
        with self.assertRaises(ContentError):
            validate_frame({**frame, "extension": True})
        with self.assertRaises(ContentError):
            validate_frame({**frame, "v": True})
        with self.assertRaises(ContentError):
            validate_frame({**frame, "cids": [CID, CID]})
        with self.assertRaises(ContentError):
            validate_frame({
                **found_frame(REQUEST, CID, [PROVIDER], ttl_ms=1),
                "providers": [PROVIDER, PROVIDER],
            })
        with self.assertRaises(ContentError):
            found_frame(REQUEST, CID, [], ttl_ms=True)


if __name__ == "__main__":
    unittest.main()
