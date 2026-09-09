"""Crash-safe filesystem repository for Netz durable blobs.

The on-disk piece and installation-marker layout is byte-compatible with the
JVM reference store. One process must exclusively own a root. Public methods
are asynchronous so filesystem work does not block an asyncio carrier.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import os
from pathlib import Path
import stat
import tempfile
import time
from typing import Any, Callable

from netz_codec import dumps, loads
from .directory_sync import provision_directory

from .blob import (
    OBJECT_TAG,
    expected_piece_length,
    manifest_address,
    object_address,
    validate_manifest,
    verify_piece,
)

BEST_EFFORT = "netz/store/best-effort/1"
FSYNC = "netz/store/fsync/1"
PERSISTENCE_PROFILES = frozenset({BEST_EFFORT, FSYNC})
MAX_MARKER_BYTES = 2 * 1024 * 1024
MAX_PAGE_SIZE = 1024
MAX_SAFE_INTEGER = (1 << 53) - 1
MARKER_FIELDS = frozenset({
    "v", "type", "manifest", "object", "descriptor", "persistence",
    "stored_at",
})


class BlobStoreError(RuntimeError):
    """A durable repository input, entry, or persistence claim is invalid."""

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code = code


def _bytes32(value: bytes | str, label: str) -> bytes:
    if isinstance(value, bytes) and len(value) == 32:
        return value
    if (isinstance(value, str) and len(value) == 64
            and value == value.lower()
            and all(character in "0123456789abcdef" for character in value)):
        return bytes.fromhex(value)
    raise BlobStoreError(f"invalid {label} address", "invalid-address")


def _safe_uint(value: Any) -> bool:
    return (isinstance(value, int) and not isinstance(value, bool)
            and 0 <= value <= MAX_SAFE_INTEGER)


class FileBlobStore:
    """Asynchronous, content-addressed filesystem blob repository.

    ``fsync`` is the default persistence profile and requires an explicit
    ``durable_ancestor`` whose own name is already durable. The managed tree
    must be exclusively owned; directory barriers currently require POSIX.
    It repeats parent-chain barriers on writes/retries and forces staged pieces,
    atomic installation markers, and their containing directories before a
    method reports success. ``best-effort`` retains atomic replacement but
    makes no crash-durability claim. Cancelling an await does not roll back a
    filesystem operation already running; a new session resumes by verifying
    the addressed pieces.
    """

    def __init__(self, root: str | os.PathLike[str], *,
                 persistence: str = FSYNC,
                 durable_ancestor: str | os.PathLike[str] | None = None,
                 clock: Callable[[], int] | None = None,
                 max_inventory_records: int = 1_000_000) -> None:
        if persistence not in PERSISTENCE_PROFILES:
            raise BlobStoreError("invalid persistence profile",
                                 "invalid-options")
        if (not isinstance(max_inventory_records, int)
                or isinstance(max_inventory_records, bool)
                or max_inventory_records <= 0):
            raise BlobStoreError("invalid inventory bound", "invalid-options")
        if clock is not None and not callable(clock):
            raise BlobStoreError("invalid store clock", "invalid-options")
        self.root = Path(os.path.abspath(Path(root).expanduser()))
        self._durable_ancestor = (Path(os.path.abspath(durable_ancestor))
                                  if durable_ancestor is not None and os.fspath(durable_ancestor)
                                  else durable_ancestor)
        if persistence == FSYNC:
            if durable_ancestor is None:
                raise BlobStoreError("fsync requires an explicit durable_ancestor", "invalid-options")
            provision_directory(self._durable_ancestor, self.root)
        else:
            self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir():
            raise BlobStoreError("blob-store root is not a directory",
                                 "invalid-root")
        self.persistence = persistence
        self._fsync = persistence == FSYNC
        self._clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._max_inventory_records = max_inventory_records
        # One owned worker keeps blocking IO off the event loop and serializes
        # every mutation under the exclusive-root contract.
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="netz-blob-store")
        self._closed = False

    async def _run(self, operation: Callable[[], Any]) -> Any:
        if self._closed:
            raise BlobStoreError("blob store is closed", "store-closed")
        # Polling also works in embedded loops whose cross-thread wakeup
        # channel is disabled. The sleep keeps the loop available to the
        # carrier while a bounded filesystem operation is in progress.
        worker = self._executor.submit(operation)
        while not worker.done():
            await asyncio.sleep(0.001)
        return worker.result()

    async def close(self) -> None:
        """Mark the repository closed after all prior calls have completed."""
        if not self._closed:
            self._closed = True
            self._executor.shutdown(wait=True, cancel_futures=False)

    async def __aenter__(self) -> FileBlobStore:
        if self._closed:
            raise BlobStoreError("blob store is closed", "store-closed")
        return self

    async def __aexit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        await self.close()

    @staticmethod
    def _addressed_path(root: Path, directory: str, address: bytes,
                        suffix: str) -> Path:
        spelling = address.hex()
        return root / directory / spelling[:2] / (spelling[2:] + suffix)

    def _piece_path(self, address: bytes) -> Path:
        return self._addressed_path(self.root, "pieces", address, ".piece")

    def _marker_path(self, manifest_id: bytes) -> Path:
        return self._addressed_path(
            self.root, "manifests", manifest_id, ".cbor")

    @staticmethod
    def _regular_file(path: Path) -> bool:
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISREG(mode)

    @classmethod
    def _read_bounded(cls, path: Path, limit: int) -> bytes:
        if not cls._regular_file(path):
            raise BlobStoreError("blob-store entry is not a regular file",
                                 "invalid-file-entry")
        size = path.stat(follow_symlinks=False).st_size
        if size > limit:
            raise BlobStoreError("blob-store metadata exceeds its bound",
                                 "file-entry-too-large")
        return path.read_bytes()

    @staticmethod
    def _force_path(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _atomic_write(self, target: Path, data: bytes) -> None:
        if self._fsync:
            provision_directory(self._durable_ancestor, target.parent)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".netz-", suffix=".tmp", dir=target.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(data)
                output.flush()
                if self._fsync:
                    os.fsync(output.fileno())
            os.replace(temporary, target)
            if self._fsync:
                self._force_path(target.parent)
        except OSError as error:
            raise BlobStoreError("atomic blob write failed",
                                 "atomic-write-failed") from error
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _verify_context(manifest_id: bytes | str,
                        manifest: dict[str, Any]) -> bytes:
        address = _bytes32(manifest_id, "manifest")
        validate_manifest(manifest)
        if address != manifest_address(manifest):
            raise BlobStoreError("manifest does not match its store address",
                                 "manifest-address-mismatch")
        return address

    @staticmethod
    def _expected_piece(manifest: dict[str, Any], index: int,
                        piece_id: bytes | str) -> bytes:
        address = _bytes32(piece_id, "piece")
        if (not isinstance(index, int) or isinstance(index, bool)
                or not 0 <= index < len(manifest["pieces"])
                or address != manifest["pieces"][index]):
            raise BlobStoreError("piece does not match its manifest position",
                                 "piece-address-mismatch")
        return address

    def _read_piece(self, manifest: dict[str, Any], index: int) -> bytes | None:
        target = self._piece_path(manifest["pieces"][index])
        if not target.exists():
            return None
        data = self._read_bounded(target, manifest["piece_size"])
        if (len(data) == expected_piece_length(manifest, index)
                and verify_piece(manifest, index, data)):
            return data
        return None

    def _verify_object(self, manifest: dict[str, Any]) -> None:
        digest = sha256()
        digest.update(OBJECT_TAG)
        length = 0
        for index in range(len(manifest["pieces"])):
            data = self._read_piece(manifest, index)
            if data is None:
                raise BlobStoreError(
                    f"installed object has missing or corrupt piece {index}",
                    "missing-piece")
            digest.update(data)
            length += len(data)
        if (length != manifest["length"]
                or digest.digest() != manifest["object"]):
            raise BlobStoreError("pieces do not match the object address",
                                 "object-mismatch")

    def _valid_marker(self, marker: Any, manifest_id: bytes,
                      manifest: dict[str, Any]) -> bool:
        try:
            self._verify_context(manifest_id, manifest)
            return (
                isinstance(marker, dict)
                and set(marker) == MARKER_FIELDS
                and marker["v"] == 1
                and marker["type"] == "blob/install"
                and marker["manifest"] == manifest_id
                and marker["object"] == manifest["object"]
                and dumps(marker["descriptor"]) == dumps(manifest)
                and marker["persistence"] in PERSISTENCE_PROFILES
                and _safe_uint(marker["stored_at"])
            )
        except (KeyError, TypeError, ValueError, BlobStoreError):
            return False

    def _load_marker(self, manifest_id: bytes,
                     manifest: dict[str, Any]) -> dict[str, Any] | None:
        target = self._marker_path(manifest_id)
        if not target.exists():
            return None
        try:
            marker = loads(self._read_bounded(target, MAX_MARKER_BYTES))
        except (ValueError, OSError) as error:
            raise BlobStoreError("invalid installed-object marker",
                                 "invalid-install-marker") from error
        if not self._valid_marker(marker, manifest_id, manifest):
            raise BlobStoreError("invalid installed-object marker",
                                 "invalid-install-marker")
        return marker

    @staticmethod
    def _installed(marker: dict[str, Any]) -> dict[str, Any]:
        return {
            "manifest": marker["manifest"],
            "object": marker["object"],
            "persistence": marker["persistence"],
            "stored_at": marker["stored_at"],
        }

    async def load_staging(self, session: bytes, manifest_id: bytes | str,
                           manifest: dict[str, Any]) -> dict[str, Any]:
        """Return verified held pieces and an optional installed record."""
        del session

        def operation() -> dict[str, Any]:
            address = self._verify_context(manifest_id, manifest)
            held = [piece for index, piece in enumerate(manifest["pieces"])
                    if self._read_piece(manifest, index) is not None]
            held = list(dict.fromkeys(held))
            marker = self._load_marker(address, manifest)
            installed = None
            if marker is not None and len(held) == len(set(manifest["pieces"])):
                self._verify_object(manifest)
                installed = self._installed(marker)
            return {"held": held, "installed": installed}

        return await self._run(operation)

    async def stage_piece(self, session: bytes, manifest_id: bytes | str,
                          manifest: dict[str, Any], index: int,
                          piece_id: bytes | str, data: bytes) -> str:
        """Verify and durably stage one content-addressed piece."""
        del session

        def operation() -> str:
            self._verify_context(manifest_id, manifest)
            address = self._expected_piece(manifest, index, piece_id)
            if not verify_piece(manifest, index, data):
                raise BlobStoreError("refusing to stage an invalid piece",
                                     "invalid-piece")
            target = self._piece_path(address)
            if self._regular_file(target) and self._read_piece(manifest, index):
                if self._fsync:
                    provision_directory(self._durable_ancestor, target.parent)
                    self._force_path(target)
                    self._force_path(target.parent)
            else:
                self._atomic_write(target, data)
            if self._read_piece(manifest, index) is None:
                raise BlobStoreError("piece failed verification after staging",
                                     "stage-verification-failed")
            return "ok"

        return await self._run(operation)

    async def install_object(self, session: bytes, manifest_id: bytes | str,
                             manifest: dict[str, Any], retention: Any,
                             persistence: str) -> dict[str, Any]:
        """Atomically publish an installation marker after full verification."""
        del session, retention

        def operation() -> dict[str, Any]:
            address = self._verify_context(manifest_id, manifest)
            if persistence != self.persistence:
                raise BlobStoreError(
                    "requested persistence does not match the file store",
                    "persistence-mismatch")
            self._verify_object(manifest)
            existing = self._load_marker(address, manifest)
            if existing is not None and existing["persistence"] == persistence:
                if self._fsync:
                    self._force_pieces(manifest)
                    target = self._marker_path(address)
                    provision_directory(self._durable_ancestor, target.parent)
                    self._force_path(target)
                    self._force_path(target.parent)
                return {**self._installed(existing), "result": "ok"}
            if self._fsync:
                self._force_pieces(manifest)
            stored_at = self._clock()
            if not _safe_uint(stored_at):
                raise BlobStoreError("store clock returned an invalid timestamp",
                                     "invalid-stored-at")
            marker = {
                "v": 1,
                "type": "blob/install",
                "manifest": address,
                "object": manifest["object"],
                "descriptor": manifest,
                "persistence": persistence,
                "stored_at": stored_at,
            }
            self._atomic_write(self._marker_path(address), dumps(marker))
            return {**self._installed(marker), "result": "ok"}

        return await self._run(operation)

    def _force_pieces(self, manifest: dict[str, Any]) -> None:
        for piece in dict.fromkeys(manifest["pieces"]):
            target = self._piece_path(piece)
            provision_directory(self._durable_ancestor, target.parent)
            self._force_path(target)
            self._force_path(target.parent)

    async def cleanup_session(self, session: bytes,
                              manifest_id: bytes | str) -> bool:
        """Release no addressed bytes; staged pieces survive retry/restart."""
        del session, manifest_id
        if self._closed:
            raise BlobStoreError("blob store is closed", "store-closed")
        return True

    def _marker_and_manifest(
            self, manifest_id: bytes | str
    ) -> tuple[bytes, dict[str, Any] | None, dict[str, Any] | None]:
        address = _bytes32(manifest_id, "manifest")
        target = self._marker_path(address)
        if not target.exists():
            return address, None, None
        try:
            marker = loads(self._read_bounded(target, MAX_MARKER_BYTES))
            manifest = marker.get("descriptor") if isinstance(marker, dict) else None
        except (ValueError, OSError) as error:
            raise BlobStoreError("invalid installed-object marker",
                                 "invalid-install-marker") from error
        if not isinstance(manifest, dict) or not self._valid_marker(
                marker, address, manifest):
            raise BlobStoreError("invalid installed-object marker",
                                 "invalid-install-marker")
        return address, marker, manifest

    async def load_installed_manifest(
            self, manifest_id: bytes | str
    ) -> dict[str, Any] | None:
        """Load a validated installed descriptor, or ``None`` when absent."""
        def operation() -> dict[str, Any] | None:
            _address, _marker, manifest = self._marker_and_manifest(manifest_id)
            return manifest
        return await self._run(operation)

    async def load_piece(self, session: bytes, manifest_id: bytes | str,
                         index: int, piece_id: bytes | str) -> bytes:
        """Load one verified piece, but only from an installed manifest."""
        del session

        def operation() -> bytes:
            _address, marker, manifest = self._marker_and_manifest(manifest_id)
            if marker is None or manifest is None:
                raise BlobStoreError("manifest is not installed",
                                     "manifest-not-installed")
            self._expected_piece(manifest, index, piece_id)
            data = self._read_piece(manifest, index)
            if data is None:
                raise BlobStoreError("installed piece is missing or corrupt",
                                     "missing-piece")
            return data
        return await self._run(operation)

    def _installed_addresses(self) -> list[bytes]:
        root = self.root / "manifests"
        if not root.exists():
            return []
        paths: list[Path] = []
        for directory, names, files in os.walk(root, followlinks=False):
            names[:] = [name for name in names
                        if not (Path(directory) / name).is_symlink()]
            for filename in files:
                if filename.endswith(".cbor"):
                    paths.append(Path(directory) / filename)
                    if len(paths) > self._max_inventory_records:
                        raise BlobStoreError(
                            "installed-object inventory exceeds its bound",
                            "too-many-installed-objects")
        addresses: list[bytes] = []
        for path in paths:
            if not self._regular_file(path):
                raise BlobStoreError("invalid installed-manifest path",
                                     "invalid-manifest-path")
            try:
                relative = path.relative_to(root)
            except ValueError as error:
                raise BlobStoreError("invalid installed-manifest path",
                                     "invalid-manifest-path") from error
            parts = relative.parts
            spelling = (parts[0] + parts[1][:-5]) if len(parts) == 2 else ""
            if (len(parts) != 2 or len(parts[0]) != 2
                    or not parts[1].endswith(".cbor")
                    or len(spelling) != 64 or spelling != spelling.lower()
                    or any(character not in "0123456789abcdef"
                           for character in spelling)):
                raise BlobStoreError("invalid installed-manifest path",
                                     "invalid-manifest-path")
            addresses.append(bytes.fromhex(spelling))
        return addresses

    async def inventory_page(self, cursor: str | None = None,
                             limit: int = MAX_PAGE_SIZE) -> dict[str, Any]:
        """Return one sorted, exclusive-cursor page of marker addresses."""
        if (cursor is not None
                and (not isinstance(cursor, str) or len(cursor) != 64
                     or cursor != cursor.lower()
                     or any(character not in "0123456789abcdef"
                            for character in cursor))):
            raise BlobStoreError("invalid inventory cursor",
                                 "invalid-inventory-page")
        if (not isinstance(limit, int) or isinstance(limit, bool)
                or not 1 <= limit <= MAX_PAGE_SIZE):
            raise BlobStoreError("invalid inventory limit",
                                 "invalid-inventory-page")

        def operation() -> dict[str, Any]:
            spellings = sorted(set(address.hex()
                                   for address in self._installed_addresses()))
            eligible = [value for value in spellings
                        if cursor is None or value > cursor]
            window = eligible[:limit + 1]
            more = len(window) > limit
            page = window[:limit]
            return {
                "manifests": [bytes.fromhex(value) for value in page],
                "next_cursor": page[-1] if more else None,
            }

        return await self._run(operation)

    async def scrub_object(self, manifest_id: bytes | str) -> dict[str, Any]:
        """Verify the marker, every piece, and the complete object address."""
        def operation() -> dict[str, Any]:
            address, marker, manifest = self._marker_and_manifest(manifest_id)
            if marker is None or manifest is None:
                return {"status": "missing", "manifest": address}
            self._verify_object(manifest)
            return {
                "status": "healthy",
                "manifest": address,
                "object": marker["object"],
                "persistence": marker["persistence"],
                "stored_at": marker["stored_at"],
                "length": manifest["length"],
                "pieces": len(manifest["pieces"]),
            }

        return await self._run(operation)

    async def read_installed(self, manifest_id: bytes | str,
                             max_bytes: int) -> bytes:
        """Reconstruct one verified installed object under a caller bound."""
        if (not isinstance(max_bytes, int) or isinstance(max_bytes, bool)
                or max_bytes < 0):
            raise BlobStoreError("invalid read bound", "invalid-read-bound")

        def operation() -> bytes:
            _address, marker, manifest = self._marker_and_manifest(manifest_id)
            if marker is None or manifest is None:
                raise BlobStoreError("manifest is not installed",
                                     "manifest-not-installed")
            if manifest["length"] > max_bytes:
                raise BlobStoreError("installed object exceeds the read bound",
                                     "object-too-large")
            parts = []
            for index in range(len(manifest["pieces"])):
                data = self._read_piece(manifest, index)
                if data is None:
                    raise BlobStoreError(
                        "installed piece is missing or corrupt", "missing-piece")
                parts.append(data)
            value = b"".join(parts)
            if (len(value) != manifest["length"]
                    or object_address(value) != manifest["object"]):
                raise BlobStoreError("pieces do not match the object address",
                                     "object-mismatch")
            return value

        return await self._run(operation)


__all__ = [
    "BEST_EFFORT",
    "FSYNC",
    "BlobStoreError",
    "FileBlobStore",
    "MAX_PAGE_SIZE",
]
