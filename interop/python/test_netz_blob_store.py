import asyncio
from pathlib import Path
import tempfile
import unittest

from netz_codec import loads
from netz_protocol.blob import split_blob
from netz_protocol.blob_store import (
    BEST_EFFORT,
    FSYNC,
    BlobStoreError,
    FileBlobStore,
)


ROOT = Path(__file__).resolve().parents[2]
SESSION = bytes.fromhex("30" * 16)
SESSION_2 = bytes.fromhex("31" * 16)


class PythonFileBlobStoreTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix="netz-python-blob-store-")
        self.root = Path(self.temporary.name)
        self.split = split_blob(bytes(range(20)), 8)
        self.stores = []

    async def asyncTearDown(self):
        for store in self.stores:
            await store.close()
        self.temporary.cleanup()

    def new_store(self, **options):
        store = FileBlobStore(self.root, **options)
        self.stores.append(store)
        return store

    async def stage(self, store, piece):
        return await store.stage_piece(
            SESSION, self.split["manifest_id"], self.split["manifest"],
            piece["index"], piece["id"], piece["data"])

    async def install(self, store, persistence=FSYNC):
        return await store.install_object(
            SESSION, self.split["manifest_id"], self.split["manifest"],
            {}, persistence)

    def addressed_path(self, directory, address, suffix):
        spelling = address.hex()
        return self.root / directory / spelling[:2] / (spelling[2:] + suffix)

    async def test_staging_install_restart_and_jvm_marker_format(self):
        store = self.new_store(clock=lambda: 1200)
        self.assertEqual(
            {"held": [], "installed": None},
            await store.load_staging(
                SESSION, self.split["manifest_id"], self.split["manifest"]),
        )
        self.assertEqual("ok", await self.stage(store, self.split["pieces"][0]))
        self.assertTrue(await store.cleanup_session(
            SESSION, self.split["manifest_id"]))

        restarted = self.new_store(clock=lambda: 1200)
        partial = await restarted.load_staging(
            SESSION_2, self.split["manifest_id"], self.split["manifest"])
        self.assertEqual([self.split["pieces"][0]["id"]], partial["held"])
        self.assertIsNone(partial["installed"])

        await asyncio.gather(*(
            self.stage(restarted, piece)
            for piece in self.split["pieces"][1:]
        ))
        installed = await self.install(restarted)
        self.assertEqual("ok", installed["result"])
        self.assertEqual(FSYNC, installed["persistence"])
        self.assertEqual(1200, installed["stored_at"])

        marker_path = self.addressed_path(
            "manifests", self.split["manifest_id"], ".cbor")
        fixture_hex = (
            ROOT / "spec/fixtures/blob-file-store-marker-1.hex"
        ).read_text().strip()
        self.assertEqual(fixture_hex, marker_path.read_bytes().hex())
        marker = loads(marker_path.read_bytes())
        self.assertEqual(self.split["manifest"], marker["descriptor"])
        for piece in self.split["pieces"]:
            self.assertEqual(
                piece["data"],
                self.addressed_path("pieces", piece["id"], ".piece").read_bytes(),
            )

        complete = await restarted.load_staging(
            SESSION_2, self.split["manifest_id"], self.split["manifest"])
        self.assertEqual(FSYNC, complete["installed"]["persistence"])
        self.assertEqual(
            self.split["pieces"][1]["data"],
            await restarted.load_piece(
                SESSION_2, self.split["manifest_id"], 1,
                self.split["pieces"][1]["id"]),
        )
        self.assertEqual(bytes(range(20)), await restarted.read_installed(
            self.split["manifest_id"], 20))

    async def test_partial_corrupt_and_mismatched_claims_fail_closed(self):
        store = self.new_store(
            persistence=BEST_EFFORT, clock=lambda: 1200)
        with self.assertRaises(BlobStoreError) as raised:
            await store.stage_piece(
                SESSION, self.split["manifest_id"], self.split["manifest"],
                0, self.split["pieces"][0]["id"], b"invalid")
        self.assertEqual("invalid-piece", raised.exception.code)

        for piece in self.split["pieces"][:-1]:
            await self.stage(store, piece)
        with self.assertRaises(BlobStoreError) as raised:
            await self.install(store, BEST_EFFORT)
        self.assertEqual("missing-piece", raised.exception.code)

        await self.stage(store, self.split["pieces"][-1])
        with self.assertRaises(BlobStoreError) as raised:
            await self.install(store, FSYNC)
        self.assertEqual("persistence-mismatch", raised.exception.code)
        await self.install(store, BEST_EFFORT)

        damaged = self.addressed_path(
            "pieces", self.split["pieces"][1]["id"], ".piece")
        damaged.write_bytes(b"\x00" * 8)
        with self.assertRaises(BlobStoreError) as raised:
            await store.scrub_object(self.split["manifest_id"])
        self.assertEqual("missing-piece", raised.exception.code)
        staging = await store.load_staging(
            SESSION_2, self.split["manifest_id"], self.split["manifest"])
        self.assertNotIn(self.split["pieces"][1]["id"], staging["held"])
        self.assertIsNone(staging["installed"])

    async def test_inventory_is_bounded_sorted_and_exclusive(self):
        store = self.new_store(clock=lambda: 1200)
        second = split_blob(bytes(range(30, 55)), 8)

        async def install(value):
            for piece in value["pieces"]:
                await store.stage_piece(
                    SESSION, value["manifest_id"], value["manifest"],
                    piece["index"], piece["id"], piece["data"])
            await store.install_object(
                SESSION, value["manifest_id"], value["manifest"], {}, FSYNC)

        await install(self.split)
        await install(second)
        first = await store.inventory_page(limit=1)
        following = await store.inventory_page(first["next_cursor"], 1)
        self.assertEqual(
            sorted([self.split["manifest_id"], second["manifest_id"]]),
            first["manifests"] + following["manifests"],
        )
        self.assertIsNotNone(first["next_cursor"])
        self.assertIsNone(following["next_cursor"])
        self.assertEqual(
            "healthy", (await store.scrub_object(
                self.split["manifest_id"]))["status"])
        self.assertEqual(
            self.split["manifest"],
            await store.load_installed_manifest(self.split["manifest_id"]),
        )
        missing = bytes.fromhex("ff" * 32)
        self.assertEqual("missing", (await store.scrub_object(missing))["status"])
        self.assertIsNone(await store.load_installed_manifest(missing))

        with self.assertRaises(BlobStoreError):
            await store.inventory_page("FF" * 32, 1)
        with self.assertRaises(BlobStoreError):
            await store.inventory_page(None, 1025)
        bounded = self.new_store(
            clock=lambda: 1200, max_inventory_records=1)
        with self.assertRaises(BlobStoreError) as raised:
            await bounded.inventory_page()
        self.assertEqual("too-many-installed-objects", raised.exception.code)
        with self.assertRaises(BlobStoreError) as raised:
            await store.read_installed(self.split["manifest_id"], 19)
        self.assertEqual("object-too-large", raised.exception.code)

    async def test_marker_corruption_and_inventory_path_tampering_are_rejected(self):
        store = self.new_store(clock=lambda: 1200)
        for piece in self.split["pieces"]:
            await self.stage(store, piece)
        await self.install(store)
        marker = self.addressed_path(
            "manifests", self.split["manifest_id"], ".cbor")
        marker.write_bytes(b"not-cbor")
        with self.assertRaises(BlobStoreError) as raised:
            await store.load_installed_manifest(self.split["manifest_id"])
        self.assertEqual("invalid-install-marker", raised.exception.code)

        invalid = self.root / "manifests" / "bad" / "entry.cbor"
        invalid.parent.mkdir(parents=True, exist_ok=True)
        invalid.write_bytes(b"x")
        with self.assertRaises(BlobStoreError) as raised:
            await store.inventory_page()
        self.assertEqual("invalid-manifest-path", raised.exception.code)

    async def test_options_and_closed_state_fail_closed(self):
        with self.assertRaises(BlobStoreError):
            self.new_store(clock=1200)
        store = self.new_store(clock=lambda: 1200)
        await store.close()
        with self.assertRaises(BlobStoreError) as raised:
            await store.inventory_page()
        self.assertEqual("store-closed", raised.exception.code)
        with self.assertRaises(BlobStoreError):
            await store.cleanup_session(SESSION, self.split["manifest_id"])


if __name__ == "__main__":
    unittest.main()
