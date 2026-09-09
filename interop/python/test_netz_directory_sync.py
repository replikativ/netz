import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from netz_protocol import directory_sync as ds


@unittest.skipUnless(os.name == "posix", "POSIX directory barrier qualification")
class DirectorySyncTest(unittest.TestCase):
    def test_retry_repeats_barriers_and_failure_stops_descent(self):
        for fail_at in range(1, 6):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                a, b = root / "a", root / "a" / "b"
                expected = [root, root, a, a, b]
                calls = []
                error = PermissionError("injected barrier failure")

                def barrier(path):
                    calls.append(path)
                    if len(calls) == fail_at:
                        raise error

                with patch.object(ds, "sync_directory", side_effect=barrier):
                    with self.assertRaises(PermissionError) as caught:
                        ds.provision_directory(root, b)
                self.assertIs(error, caught.exception)
                self.assertEqual(expected[:fail_at], calls)
                self.assertEqual(fail_at > 1, a.exists())
                self.assertEqual(fail_at > 3, b.exists())
                for _ in range(2):
                    with patch.object(ds, "sync_directory") as barrier:
                        self.assertEqual(b, ds.provision_directory(root, b))
                        self.assertEqual(expected, [c.args[0] for c in barrier.call_args_list])

    def test_invalid_boundaries_do_not_sync_or_create(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch.object(ds, "sync_directory") as barrier:
                for target in ["", root.parent / "outside", root / ".." / "outside"]:
                    with self.assertRaises(ValueError):
                        ds.provision_directory(root, target)
                with self.assertRaises(FileNotFoundError):
                    ds.provision_directory(root / "missing", root / "missing" / "child")
                barrier.assert_not_called()
                self.assertFalse((root / "missing").exists())

    def test_symlink_and_file_components_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "real").mkdir()
            (root / "link").symlink_to(root / "real", target_is_directory=True)
            (root / "file").touch()
            for component in ["link", "file"]:
                with self.assertRaises(ValueError):
                    ds.provision_directory(root, root / component / "child")
            self.assertFalse((root / "real" / "child").exists())

    def test_real_barriers_and_idempotent_reopen(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertEqual(root, ds.provision_directory(root, root))
            for _ in range(2):
                self.assertEqual(root / "a" / "b",
                                 ds.provision_directory(root, root / "a" / "b"))
