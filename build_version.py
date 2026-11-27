"""Derive Netz's commit-count release version without build dependencies."""

from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parent
PREFIX = (ROOT / "VERSION_PREFIX").read_text(encoding="utf-8").strip()


def build_version() -> str:
    override = os.environ.get("NETZ_BUILD_VERSION")
    if override:
        version = override
    elif (ROOT / ".git").exists():
        if subprocess.check_output(
            ["git", "rev-parse", "--is-shallow-repository"], cwd=ROOT, text=True
        ).strip() == "true":
            raise RuntimeError("A commit-count build requires full Git history")
        count = subprocess.check_output(
            ["git", "rev-list", "--count", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip()
        version = f"{PREFIX}.{count}"
    else:
        metadata = ROOT / "PKG-INFO"
        if not metadata.is_file():
            raise RuntimeError(
                "Git metadata is unavailable; set NETZ_BUILD_VERSION for an export"
            )
        match = re.search(
            r"^Version: ([^\r\n]+)$",
            metadata.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
        if match is None:
            raise RuntimeError(f"version is missing from {metadata}")
        version = match.group(1)
    if not re.fullmatch(rf"{re.escape(PREFIX)}\.[1-9][0-9]*", version):
        raise RuntimeError(f"invalid Netz build version: {version!r}")
    return version


if __name__ == "__main__":
    print(build_version())
