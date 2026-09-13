"""Build metadata for commit-count Netz Python releases."""

from __future__ import annotations

from pathlib import Path
import sys

from setuptools import setup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_version import build_version


setup(version=build_version())
