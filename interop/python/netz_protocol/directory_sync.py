"""Directory barriers for explicitly owned, trusted POSIX filesystem trees.

The caller must exclude concurrent renames, deletion and symlink replacement.
This is not a sandbox against hostile filesystem mutation. Successful fsync
calls do not independently qualify an OS/filesystem's power-loss behavior.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat


def _path(value: str | os.PathLike[str]) -> Path:
    raw = os.fspath(value)
    if not isinstance(raw, str) or not raw:
        raise ValueError("directory provisioning requires a nonempty text path")
    # Lexical normalization, not resolve(): resolving would hide symlinks.
    return Path(os.path.abspath(raw))


def _directory(path: Path) -> None:
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise ValueError(f"not a real directory component: {path}")


def sync_directory(path: str | os.PathLike[str]) -> None:
    """Force directory entries; permission and unsupported-operation errors escape."""
    if os.name != "posix":
        raise NotImplementedError("Python directory barriers currently require POSIX")
    target = _path(path)
    _directory(target)
    descriptor = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def provision_directory(ancestor: str | os.PathLike[str],
                        path: str | os.PathLike[str]) -> Path:
    """Provision beneath an existing ancestor whose name is already durable.

    Repeat every barrier on retry, including existing directory components.
    Failure may leave directories behind: existence never proves durability.
    Never infer the trusted ancestor by walking upward from the target.
    """
    root, target = _path(ancestor), _path(path)
    relative = target.relative_to(root)
    if os.name != "posix":
        raise NotImplementedError("Python directory provisioning currently requires POSIX")
    _directory(root)
    sync_directory(root)
    parent = root
    for component in relative.parts:
        child = parent / component
        try:
            child.mkdir()
        except FileExistsError:
            pass
        _directory(child)
        sync_directory(parent)
        sync_directory(child)
        parent = child
    return target
