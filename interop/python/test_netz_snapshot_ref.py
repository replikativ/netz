from __future__ import annotations

import unittest

from netz_protocol.identity import genesis
from netz_protocol.snapshot_ref import (
    SnapshotRefError,
    SnapshotRefState,
    decode_snapshot_ref,
    encode_snapshot_ref,
    sign_snapshot_ref,
    signing_bytes,
    verify_snapshot_ref,
)


PRIVATE = bytes(range(32))


def authority_genesis() -> dict[str, object]:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    public = (Ed25519PrivateKey.from_private_bytes(PRIVATE)
              .public_key().public_bytes_raw())
    return genesis([b"r" * 32], 1, [public], b"x" * 32)


def signed(snapshot: bytes, parents: list[bytes], *, ref: bytes = b"main"):
    return sign_snapshot_ref(
        authority_genesis(), PRIVATE,
        profile="datahike/commit-v1", system=b"store-a", ref=ref,
        snapshot=snapshot, parents=parents,
    )


class SnapshotRefTests(unittest.TestCase):
    def test_known_signing_bytes(self):
        actual = signing_bytes(
            profile="datahike/commit-v1", system=b"store-a", ref=b"main",
            snapshot=b"child", parents=[b"parent-b", b"parent-a", b"parent-b"],
            publisher=bytes.fromhex("aa" * 32),
        )
        self.assertEqual(
            actual.hex(),
            "6e65747a2f736e617073686f742d7265662f763100"
            "a861760163726566446d61696e64747970656c736e617073686f742d726566"
            "6673797374656d4773746f72652d6167706172656e74738248706172656e742d61"
            "48706172656e742d626770726f66696c65726461746168696b652f636f6d6d6974"
            "2d763168736e617073686f74456368696c64697075626c69736865725820"
            + "aa" * 32,
        )

    def test_round_trip_tamper_and_dag_order(self):
        child = signed(b"child", [b"parent"])
        parent = signed(b"parent", [])
        decoded = decode_snapshot_ref(encode_snapshot_ref(child))
        self.assertEqual(decoded["snapshot"], b"child")
        state = SnapshotRefState()
        self.assertEqual(state.accept(child), "accepted")
        self.assertEqual(state.accept(parent), "accepted")
        stream = ("datahike/commit-v1", b"store-a", b"main")
        self.assertEqual(state.heads[stream], {b"child"})
        self.assertEqual(state.accept(child), "duplicate")
        tampered = {**child, "parents": [b"other"]}
        with self.assertRaises(SnapshotRefError):
            verify_snapshot_ref(tampered)

    def test_concurrent_heads_merge_and_wrong_stream(self):
        state = SnapshotRefState()
        for item in [signed(b"base", []),
                     signed(b"left", [b"base"]),
                     signed(b"right", [b"base"])]:
            self.assertEqual(state.accept(item), "accepted")
        stream = ("datahike/commit-v1", b"store-a", b"main")
        self.assertEqual(state.heads[stream], {b"left", b"right"})
        self.assertEqual(state.accept(signed(b"merge", [b"left", b"right"])),
                         "accepted")
        self.assertEqual(state.heads[stream], {b"merge"})


if __name__ == "__main__":
    unittest.main()
