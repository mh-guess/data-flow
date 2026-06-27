"""
Pytest wrapper for the research-parity regression (parity_heuristic.py).

Layer A (sector parity on the 135-ticker oracle) is fully offline and MUST be
135/135. Layer B (raw-Tiingo industry parity) needs S3/creds; when unavailable
it is skipped, and when available it must have zero UNDOCUMENTED divergences.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

import parity_heuristic as ph

_ORACLE = ph._oracle_path()
_have_oracle = os.path.exists(_ORACLE)

pytestmark = pytest.mark.skipif(
    not _have_oracle, reason=f"oracle csv not found at {_ORACLE}"
)


def test_sector_parity_135_of_135():
    res = ph.run(verbose=False)
    assert res["sector_match"] == res["sector_total"], (
        f"sector parity {res['sector_match']}/{res['sector_total']}"
    )
    assert res["sector_total"] == 135


def test_no_undocumented_industry_divergences():
    res = ph.run(verbose=False)
    if res["industry_total"] == 0:
        pytest.skip("no Tiingo meta / S3 access; industry parity not exercised")
    assert res["undocumented_divergences"] == 0, (
        "undocumented industry divergences present — update "
        "KNOWN_INDUSTRY_DIVERGENCES or the crosswalk"
    )
