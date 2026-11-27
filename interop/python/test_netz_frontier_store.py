import asyncio
import os
import threading
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
            store = FileFrontierStore(path, durable_ancestor=root)
            self.assertEqual({}, await store.load_frontiers())
            await store.save_frontiers({stream: 11})
            restored = FileFrontierStore(path)
            self.assertEqual({stream: 11}, await restored.load_frontiers())
            self.assertEqual(decode_frontiers(path.read_bytes()), {stream: 11})

    async def test_cancelled_save_finishes_before_next_save(self):
        stream = (bytes(32), 0, "topic")
        with tempfile.TemporaryDirectory() as root:
            store = FileFrontierStore(Path(root) / "frontiers", durable_ancestor=root)
            started = threading.Event()
            release = threading.Event()
            save = store._save

            def delay_first(frontiers):
                if frontiers[stream] == 1:
                    started.set()
                    if not release.wait(5):
                        raise TimeoutError("first save did not resume")
                save(frontiers)

            with patch.object(store, "_save", side_effect=delay_first):
                first = asyncio.create_task(store.save_frontiers({stream: 1}))
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 2))
                    first.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await first
                    self.assertTrue(store._lock.locked())
                    second = asyncio.create_task(store.save_frontiers({stream: 2}))
                    await asyncio.sleep(0)
                    self.assertFalse(second.done())
                finally:
                    release.set()
                await second
            self.assertEqual({stream: 2}, await store.load_frontiers())

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires POSIX FIFOs")
    async def test_fifo_is_rejected_before_waiting_for_a_writer(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "frontiers"
            os.mkfifo(path)
            try:
                with self.assertRaises(FrontierStoreError):
                    await asyncio.wait_for(FileFrontierStore(path).load_frontiers(), 2)
            finally:
                # Also release a blocked reader if the regression returns.
                try:
                    writer = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
                except OSError:
                    pass
                else:
                    os.close(writer)

    async def test_missing_ancestor_is_read_only_without_mutation(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "nested" / "frontiers.cbor"
            store = FileFrontierStore(path)
            self.assertEqual({}, await store.load_frontiers())
            with self.assertRaises(FrontierStoreError):
                await store.save_frontiers({})
            self.assertFalse(path.parent.exists())

    async def test_failed_provisioning_does_not_publish_frontiers(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "a" / "b" / "frontiers.cbor"
            store = FileFrontierStore(path, durable_ancestor=root)
            error = PermissionError("injected parent barrier failure")
            with patch("netz_protocol.directory_sync.sync_directory", side_effect=error):
                with self.assertRaises(PermissionError):
                    await store.save_frontiers({})
            self.assertFalse(path.exists())
            await store.save_frontiers({})
            self.assertEqual({}, await store.load_frontiers())

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
