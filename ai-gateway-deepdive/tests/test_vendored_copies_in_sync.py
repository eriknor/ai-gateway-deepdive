"""Enforce byte-identical parity between canonical src/aigw/ files and their
vendored copies in app/aigw/.

The Databricks App runtime is self-contained and imports from app/aigw/ directly.
Notebooks and tests import from src/aigw/. Both copies must stay in sync; any
change to one must be applied to the other. This test catches future drift.
"""
import pathlib

_REPO_ROOT = pathlib.Path(__file__).parent.parent

_PAIRS = [
    ("src/aigw/result.py", "app/aigw/result.py"),
    ("src/aigw/queries.py", "app/aigw/queries.py"),
]


def test_vendored_copies_byte_identical():
    for src_rel, app_rel in _PAIRS:
        src_path = _REPO_ROOT / src_rel
        app_path = _REPO_ROOT / app_rel
        assert src_path.exists(), f"Canonical file missing: {src_path}"
        assert app_path.exists(), f"Vendored copy missing: {app_path}"
        src_bytes = src_path.read_bytes()
        app_bytes = app_path.read_bytes()
        assert src_bytes == app_bytes, (
            f"Vendored copy drift detected:\n"
            f"  canonical : {src_path}\n"
            f"  vendored  : {app_path}\n"
            f"Apply any change to BOTH files to keep them byte-identical."
        )
