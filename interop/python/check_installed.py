"""Verify the installed wheel, never the adjacent checkout implementation.

Run with ``python -I interop/python/check_installed.py [--core-only]``.
Test files retain their original paths so they can read normative fixtures,
but their directory is deliberately never added to the import search path.
"""

import argparse
import importlib
import importlib.metadata
import importlib.util
import pkgutil
from pathlib import Path
import sys
import unittest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-only", action="store_true")
    args = parser.parse_args()
    if not sys.flags.isolated:
        parser.error("use python -I to exclude checkout paths and PYTHONPATH")

    distribution = importlib.metadata.distribution("netz-protocol")
    if distribution.metadata.get("License-Expression") != "Apache-2.0":
        raise RuntimeError("wheel metadata must declare Apache-2.0")
    installed_files = {
        Path(distribution.locate_file(entry)).resolve()
        for entry in distribution.files or ()
    }

    def check_imports() -> None:
        for name, module in list(sys.modules.items()):
            if name in {"netz_codec", "netz_sync", "netz_protocol"} or name.startswith("netz_protocol."):
                path = Path(module.__file__).resolve()
                if path not in installed_files:
                    raise RuntimeError(f"{name} came from outside the installed wheel: {path}")

    package = importlib.import_module("netz_protocol")
    for item in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        importlib.import_module(item.name)
    check_imports()
    if not any(path.name == "py.typed" for path in installed_files):
        raise RuntimeError("wheel is missing py.typed")
    if not any(path.name == "LICENSE" and "licenses" in path.parts for path in installed_files):
        raise RuntimeError("wheel is missing its license file")
    sample = {"v": 1, "opaque": b"\x00\xff", "values": [True, None, 42]}
    if package.loads(package.dumps(sample)) != sample:
        raise RuntimeError("installed codec failed round trip")
    print(f"Verified installed netz-protocol {distribution.version}: {package.__file__}", flush=True)
    if args.core_only:
        return 0

    suite = unittest.TestSuite()
    for path in sorted(Path(__file__).resolve().parent.glob("test_*.py")):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[path.stem] = module
        spec.loader.exec_module(module)
        suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(module))
    if not suite.countTestCases():
        raise RuntimeError("installed-wheel gate discovered zero tests")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    check_imports()
    return 0 if result.wasSuccessful() and not result.skipped else 1


if __name__ == "__main__":
    raise SystemExit(main())
