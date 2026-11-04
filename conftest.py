"""Pytest bootstrap: make ``src`` and the ``tests`` dir importable without an
editable install, and expose the record factory as a fixture.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent
for _p in (_ROOT / "src", _ROOT / "tests"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pytest  # noqa: E402

from fakes import make_record  # noqa: E402  (tests/ now on sys.path)


@pytest.fixture
def record_factory():
    return make_record
