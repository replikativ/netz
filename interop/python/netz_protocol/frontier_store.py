"""Crash-safe local storage for committed dissemination frontiers."""

from __future__ import annotations

import asyncio
import errno
import os
from pathlib import Path
import stat
import tempfile
from typing import Mapping, Protocol, runtime_checkable

from netz_codec import MAX_FRAME_BYTES, dumps, loads

from .dissemination import MAX_STREAMS, Stream, have_frame
from .directory_sync import provision_directory, sync_directory
from .publication import MAX_SAFE_INTEGER

FRONTIER_TYPE = "netz/dissemination-frontiers"


class FrontierStoreError(RuntimeError):
    """A frontier store input, record, or durability operation is invalid."""


def _entries(frontiers: Mapping[Stream, int]) -> list[dict[str, object]]:
    if not isinstance(frontiers, Mapping) or len(frontiers) > MAX_STREAMS:
        raise FrontierStoreError("too many committed dissemination streams")
    entries: list[dict[str, object]] = []
    for stream, through in frontiers.items():
        if (not isinstance(stream, tuple) or len(stream) != 3
                or type(through) is not int
                or not 0 <= through <= MAX_SAFE_INTEGER):
            raise FrontierStoreError("invalid committed dissemination frontier")
        origin, epoch, topic = stream
        # Reuse the strict portable stream validators, including Authority,
        # safe-integer and UTF-8 topic bounds.
        checked = have_frame([{
            "origin": origin, "epoch": epoch, "topic": topic,
            "ranges": [[through, through]], "floor": through,
        }])["streams"][0]
        entries.append({
            "origin": checked["origin"], "epoch": checked["epoch"],
            "topic": checked["topic"], "through": through,
        })
    entries.sort(key=lambda value: (
        value["origin"], value["epoch"], value["topic"].encode("utf-8")))
    return entries


def encode_frontiers(frontiers: Mapping[Stream, int]) -> bytes:
    """Encode the closed deterministic local frontier record."""
    return dumps({"v": 1, "type": FRONTIER_TYPE,
                  "streams": _entries(frontiers)})


def decode_frontiers(encoded: bytes) -> dict[Stream, int]:
    """Decode and validate one exact frontier record."""
    if not isinstance(encoded, bytes) or len(encoded) > MAX_FRAME_BYTES:
        raise FrontierStoreError("invalid frontier record size")
    try:
        value = loads(encoded)
    except Exception as error:
        raise FrontierStoreError("invalid frontier record encoding") from error
    if (not isinstance(value, dict)
            or set(value) != {"v", "type", "streams"}
            or value.get("v") != 1 or value.get("type") != FRONTIER_TYPE
            or not isinstance(value.get("streams"), list)
            or len(value["streams"]) > MAX_STREAMS):
        raise FrontierStoreError("invalid frontier record schema")
    frontiers: dict[Stream, int] = {}
    for entry in value["streams"]:
        if not isinstance(entry, dict) or set(entry) != {
                "origin", "epoch", "topic", "through"}:
            raise FrontierStoreError("invalid frontier stream schema")
        candidate = {(entry["origin"], entry["epoch"], entry["topic"]):
                     entry["through"]}
        checked = _entries(candidate)[0]
        stream = (checked["origin"], checked["epoch"], checked["topic"])
        if stream in frontiers:
            raise FrontierStoreError("duplicate committed dissemination stream")
        frontiers[stream] = checked["through"]
    if encode_frontiers(frontiers) != encoded:
        raise FrontierStoreError("frontier record is not canonical")
    return frontiers


@runtime_checkable
class FrontierStore(Protocol):
    async def load_frontiers(self) -> Mapping[Stream, int]: ...

    async def save_frontiers(self, frontiers: Mapping[Stream, int]) -> None: ...


class MemoryFrontierStore:
    """Process-local reference store useful for ephemeral clients and tests."""

    def __init__(self, frontiers: Mapping[Stream, int] | None = None) -> None:
        self._encoded = encode_frontiers(frontiers or {})

    async def load_frontiers(self) -> Mapping[Stream, int]:
        return decode_frontiers(self._encoded)

    async def save_frontiers(self, frontiers: Mapping[Stream, int]) -> None:
        self._encoded = encode_frontiers(frontiers)


class FileFrontierStore:
    """Owned POSIX frontier file; writes require a trusted durable ancestor.

    Omitting durable_ancestor permits reads only. Construction and reads do not
    provision directories. The caller excludes concurrent filesystem mutation.
    """

    def __init__(self, path: str | os.PathLike[str], *,
                 durable_ancestor: str | os.PathLike[str] | None = None) -> None:
        expanded = Path(path).expanduser()
        self.path = expanded if expanded.is_absolute() else Path.cwd() / expanded
        self._durable_ancestor = durable_ancestor
        self._lock = asyncio.Lock()

    def _load(self) -> dict[Stream, int]:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, flags)
        except OSError as error:
            if error.errno == errno.ENOENT:
                return {}
            raise FrontierStoreError("frontier read failed") from error
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise FrontierStoreError("frontier path is not a regular file")
            with os.fdopen(descriptor, "rb") as source:
                descriptor = -1
                encoded = source.read(MAX_FRAME_BYTES + 1)
        except OSError as error:
            raise FrontierStoreError("frontier read failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        if len(encoded) > MAX_FRAME_BYTES:
            raise FrontierStoreError("frontier file exceeds its size bound")
        return decode_frontiers(encoded)

    @staticmethod
    def _force_directory(path: Path) -> None:
        sync_directory(path)

    def _save(self, frontiers: Mapping[Stream, int]) -> None:
        encoded = encode_frontiers(frontiers)
        if self._durable_ancestor is None:
            raise FrontierStoreError("frontier writes require an explicit durable_ancestor")
        provision_directory(self._durable_ancestor, self.path.parent)
        descriptor, name = tempfile.mkstemp(
            prefix=".netz-frontiers-", suffix=".tmp", dir=self.path.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path)
            self._force_directory(self.path.parent)
        except OSError as error:
            raise FrontierStoreError("atomic frontier write failed") from error
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    async def load_frontiers(self) -> Mapping[Stream, int]:
        async with self._lock:
            return await asyncio.to_thread(self._load)

    async def save_frontiers(self, frontiers: Mapping[Stream, int]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._save, dict(frontiers))


__all__ = [
    "FRONTIER_TYPE", "FileFrontierStore", "FrontierStore",
    "FrontierStoreError", "MemoryFrontierStore", "decode_frontiers",
    "encode_frontiers",
]
