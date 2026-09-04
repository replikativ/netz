import asyncio
import tempfile
import unittest
from pathlib import Path

from netz_codec import dumps
from netz_protocol import (
    FileFrontierStore,
    FrontierStoreError,
    MemoryFrontierStore,
    decode_frontiers,
    encode_frontiers,
)


class FrontierStoreTest(unittest.IsolatedAsyncioTestCase):
    async def test_canonical_frontiers_round_trip(self):
        first = (bytes([1]) * 32, 2, "db/roots")
        second = (bytes([2]) * 32, 1, "docs")
        encoded = encode_frontiers({second: 8, first: 3})
        self.assertEqual({first: 3, second: 8}, decode_frontiers(encoded))
        with self.assertRaises(FrontierStoreError):
            decode_frontiers(dumps({
                "v": 1, "type": "netz/dissemination-frontiers",
                "streams": [{"origin": first[0], "epoch": 2,
                             "topic": "db/roots", "through": 3,
                             "ignored": True}],
            }))

    async def test_file_store_persists_atomic_frontier_record(self):
        stream = (bytes([3]) * 32, 4, "db/roots")
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "frontiers.cbor"
            store = FileFrontierStore(path)
            self.assertEqual({}, await store.load_frontiers())
            await store.save_frontiers({stream: 11})
            restored = FileFrontierStore(path)
            self.assertEqual({stream: 11}, await restored.load_frontiers())
            self.assertEqual(decode_frontiers(path.read_bytes()), {stream: 11})

    async def test_memory_store_returns_an_independent_mapping(self):
        stream = (bytes([4]) * 32, 0, "db/roots")
        store = MemoryFrontierStore({stream: 1})
        loaded = dict(await store.load_frontiers())
        loaded[stream] = 9
        self.assertEqual({stream: 1}, await store.load_frontiers())

    async def test_file_store_rejects_corruption_and_symlinks(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "frontiers.cbor"
            path.write_bytes(b"not-cbor")
            with self.assertRaises(FrontierStoreError):
                await FileFrontierStore(path).load_frontiers()
            path.unlink()
            target = Path(root) / "target.cbor"
            target.write_bytes(encode_frontiers({}))
            path.symlink_to(target)
            with self.assertRaises(FrontierStoreError):
                await FileFrontierStore(path).load_frontiers()


if __name__ == "__main__":
    unittest.main()
