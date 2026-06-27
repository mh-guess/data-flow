"""
Unit tests for the heuristic hedge-map v2 (hedge_crosswalk + hedge_selection +
hedge_classification).

Covers:
  1. Crosswalk sector/industry mapping (Tiingo + GICS variants, case/whitespace).
  2. classify() ladder construction (industry → sector → SPY).
  3. resolve_classification: Tiingo primary, SIC fallback when sector null, none.
  4. build_hedge_map fallback chain (negative/low-R²/non-finite beta → demote).
  5. Schema / column conformance + rank ordering.
  6. Coverage stats.

All offline — no network, synthetic bars only.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(__file__))

import hedge_crosswalk as cw
from hedge_crosswalk import classify, sector_etf, industry_etf, SPY
from hedge_selection import (
    HEDGE_MAP_COLUMNS,
    R2_FLOOR,
    build_hedge_map,
    resolve_classification,
)
from hedge_map_flow import MIN_N_OBS, compute_coverage_stats
from hedge_classification import (
    names_reconcile,
    select_active_meta_row,
)


# ---------------------------------------------------------------------------
# Synthetic bar helpers
# ---------------------------------------------------------------------------

def _dates(n: int, end: date) -> list[date]:
    """n consecutive business-ish days ending at `end` (weekday-only)."""
    out: list[date] = []
    d = end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _bars_from_returns(rets: np.ndarray, end: date, start_price: float = 100.0) -> pd.DataFrame:
    """Build a bars DataFrame (d, close, volume) from a return series."""
    n = len(rets) + 1
    ds = _dates(n, end)
    prices = [start_price]
    for r in rets:
        prices.append(prices[-1] * (1 + r))
    return pd.DataFrame({
        "d": [x.isoformat() for x in ds],
        "close": prices,
        "volume": [1_000_000] * n,
    })


def _make_etf(end: date, seed: int, n: int = 90) -> tuple[pd.DataFrame, np.ndarray]:
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0, 0.01, n)
    return _bars_from_returns(rets, end), rets


def _make_stock_corr(etf_rets: np.ndarray, end: date, beta: float, noise: float, seed: int) -> pd.DataFrame:
    """Stock returns = beta * etf_rets + noise, aligned to the same dates."""
    rng = np.random.default_rng(seed)
    eps = rng.normal(0.0, noise, len(etf_rets))
    srets = beta * etf_rets + eps
    return _bars_from_returns(srets, end)


# ---------------------------------------------------------------------------
# 1. Crosswalk mapping
# ---------------------------------------------------------------------------

class TestSectorCrosswalk:
    def test_all_11_gics_sectors(self):
        expected = {
            "Information Technology": "XLK",
            "Financials": "XLF",
            "Health Care": "XLV",
            "Consumer Discretionary": "XLY",
            "Consumer Staples": "XLP",
            "Energy": "XLE",
            "Industrials": "XLI",
            "Materials": "XLB",
            "Utilities": "XLU",
            "Real Estate": "XLRE",
            "Communication Services": "XLC",
        }
        for sec, etf in expected.items():
            assert sector_etf(sec) == etf, sec

    def test_tiingo_sector_variants(self):
        assert sector_etf("Technology") == "XLK"
        assert sector_etf("Consumer Cyclical") == "XLY"
        assert sector_etf("Consumer Defensive") == "XLP"
        assert sector_etf("Financial Services") == "XLF"
        assert sector_etf("Healthcare") == "XLV"
        assert sector_etf("Basic Materials") == "XLB"

    def test_case_and_whitespace_insensitive(self):
        assert sector_etf("  technology ") == "XLK"
        assert sector_etf("HEALTH   CARE") == "XLV"

    def test_unknown_sector_returns_none(self):
        assert sector_etf("Nonexistent Sector") is None
        assert sector_etf(None) is None
        assert sector_etf("") is None


class TestIndustryCrosswalk:
    def test_known_pure_plays(self):
        assert industry_etf("Semiconductors") == "SMH"
        assert industry_etf("Semiconductor Equipment & Materials") == "SOXX"
        assert industry_etf("Software - Application") == "IGV"
        assert industry_etf("Software - Infrastructure") == "IGV"
        assert industry_etf("Biotechnology") == "XBI"
        assert industry_etf("Drug Manufacturers - General") == "IBB"
        assert industry_etf("Medical Devices") == "IHI"
        assert industry_etf("Banks - Regional") == "KRE"
        assert industry_etf("Aerospace & Defense") == "ITA"
        assert industry_etf("Airlines") == "JETS"
        assert industry_etf("Oil & Gas E&P") == "XOP"
        assert industry_etf("Internet Retail") == "FDN"

    def test_unmapped_industry_returns_none(self):
        assert industry_etf("Some Niche Industry") is None
        assert industry_etf(None) is None
        assert industry_etf("") is None

    def test_industry_etfs_in_candidate_set(self):
        from hedge_map_flow import ETF_CANDIDATES
        cand = set(ETF_CANDIDATES)
        for etf in cw.INDUSTRY_TO_ETF.values():
            assert etf in cand, f"{etf} not in ETF_CANDIDATES"


class TestClassifyLadder:
    def test_pure_play_ladder(self):
        c = classify("Technology", "Semiconductors")
        assert c["industry_etf"] == "SMH"
        assert c["sector_etf"] == "XLK"
        assert c["industry_source"] == "pure_play"
        assert c["rank1_etf"] == "SMH"
        assert c["rank2_etf"] == "XLK"
        assert c["rank3_etf"] == SPY

    def test_sector_fallback_ladder(self):
        c = classify("Financial Services", "Credit Services")  # industry not curated
        assert c["industry_etf"] is None
        assert c["industry_source"] == "sector_fallback"
        assert c["rank1_etf"] == "XLF"   # falls back to sector
        assert c["rank2_etf"] == "XLF"
        assert c["rank3_etf"] == SPY

    def test_unknown_sector_falls_to_spy(self):
        c = classify("Mystery Sector", "Mystery Industry")
        assert c["sector_etf"] is None
        assert c["rank1_etf"] == SPY
        assert c["rank2_etf"] == SPY
        assert c["rank3_etf"] == SPY


# ---------------------------------------------------------------------------
# 3. resolve_classification (Tiingo primary, SIC fallback)
# ---------------------------------------------------------------------------

class TestResolveClassification:
    def test_tiingo_primary(self):
        row = {"sector": "Technology", "industry": "Semiconductors",
               "sicSector": "Manufacturing", "sicIndustry": "Semiconductors & Related"}
        r = resolve_classification(row)
        assert r == {"sector": "Technology", "industry": "Semiconductors", "source": "tiingo"}

    def test_sic_fallback_when_sector_null(self):
        row = {"sector": None, "industry": None,
               "sicSector": "Manufacturing", "sicIndustry": "Pharmaceutical Preparations"}
        r = resolve_classification(row)
        assert r["source"] == "sic"
        assert r["sector"] == "Manufacturing"
        assert r["industry"] == "Pharmaceutical Preparations"

    def test_sic_fallback_when_sector_blank_string(self):
        row = {"sector": "  ", "industry": "x", "sicSector": "Services", "sicIndustry": "y"}
        r = resolve_classification(row)
        assert r["source"] == "sic"
        assert r["sector"] == "Services"

    def test_unclassified_reason_is_threaded(self):
        # No meta row → the provided unclassified_reason becomes the source.
        assert resolve_classification(None)["source"] == "no_meta"
        assert resolve_classification(None, "isactive_dropped")["source"] == "isactive_dropped"
        assert resolve_classification(None, "name_mismatch")["source"] == "name_mismatch"
        # Empty dict → no usable meta.
        assert resolve_classification({})["source"] == "no_meta"
        assert resolve_classification({"sector": "", "sicSector": ""})["source"] == "no_meta"

    def test_tiingo_sector_present_industry_null_keeps_tiingo(self):
        # If Tiingo sector exists but industry is null, we stay on Tiingo (industry None)
        # rather than mixing in a SIC industry.
        row = {"sector": "Energy", "industry": None,
               "sicSector": "Mining", "sicIndustry": "Crude Petroleum"}
        r = resolve_classification(row)
        assert r["source"] == "tiingo"
        assert r["sector"] == "Energy"
        assert r["industry"] is None


# ---------------------------------------------------------------------------
# 4 & 5. build_hedge_map: fallback chain, schema, rank ordering
# ---------------------------------------------------------------------------

class TestBuildHedgeMap:
    END = date(2026, 6, 19)

    def _scenario(self, stock_bars: pd.DataFrame, etf_bars: dict[str, pd.DataFrame],
                  meta_row: dict, sym: str = "TESTCO"):
        eligible = pd.DataFrame([{"symbol": sym, "shortable": True, "easy_to_borrow": True,
                                  "first_bar_date": date(2020, 1, 1), "eligible": True}])
        all_bars = {sym: stock_bars, **etf_bars}
        etf_meta = {e: {"shortable": True, "easy_to_borrow": True} for e in etf_bars}
        classification = {sym: meta_row}
        return build_hedge_map(eligible, all_bars, etf_meta, classification,
                               as_of=self.END, effective_date=self.END + timedelta(days=1))

    def test_pure_play_industry_selected(self):
        smh, smh_rets = _make_etf(self.END, seed=1)
        xlk, _ = _make_etf(self.END, seed=2)
        spy, _ = _make_etf(self.END, seed=3)
        # Stock strongly correlated to SMH (positive beta, high R²).
        stock = _make_stock_corr(smh_rets, self.END, beta=1.2, noise=0.002, seed=9)
        df = self._scenario(stock, {"SMH": smh, "XLK": xlk, "SPY": spy},
                            {"sector": "Technology", "industry": "Semiconductors"})
        r1 = df[df["rank"] == 1].iloc[0]
        assert r1["hedge_etf"] == "SMH"
        assert r1["selection_basis"] == "heuristic_industry"
        assert r1["industry_source"] == "pure_play"
        assert np.isfinite(r1["beta"]) and r1["beta"] > 0
        assert r1["r2"] >= R2_FLOOR

    def test_negative_beta_industry_demotes_to_sector(self):
        # Industry ETF (SMH) is NEGATIVELY correlated → rank1 demotes to sector XLK.
        smh, smh_rets = _make_etf(self.END, seed=11)
        xlk, xlk_rets = _make_etf(self.END, seed=12)
        spy, _ = _make_etf(self.END, seed=13)
        # Stock loads positively on XLK but NOT on SMH (industry tier should fail
        # the non-negative-beta / R²-floor gate and demote to the sector ETF).
        stock_xlk = _make_stock_corr(xlk_rets, self.END, beta=1.0, noise=0.001, seed=15)
        df = self._scenario(stock_xlk, {"SMH": smh, "XLK": xlk, "SPY": spy},
                            {"sector": "Technology", "industry": "Semiconductors"})
        r1 = df[df["rank"] == 1].iloc[0]
        # Industry SMH is uncorrelated/negative to stock_xlk → should demote to XLK.
        assert r1["hedge_etf"] in ("XLK", "SPY")
        assert r1["selection_basis"] in ("heuristic_sector", "spy_fallback")

    def test_low_r2_industry_demotes(self):
        # Industry ETF has near-zero correlation (low R²) → demote.
        smh, _ = _make_etf(self.END, seed=21)
        xlk, xlk_rets = _make_etf(self.END, seed=22)
        spy, _ = _make_etf(self.END, seed=23)
        stock = _make_stock_corr(xlk_rets, self.END, beta=1.0, noise=0.001, seed=24)
        df = self._scenario(stock, {"SMH": smh, "XLK": xlk, "SPY": spy},
                            {"sector": "Technology", "industry": "Semiconductors"})
        r1 = df[df["rank"] == 1].iloc[0]
        assert r1["hedge_etf"] != "SMH" or r1["r2"] >= R2_FLOOR

    def test_unclassified_stock_uses_spy(self):
        spy, spy_rets = _make_etf(self.END, seed=31)
        stock = _make_stock_corr(spy_rets, self.END, beta=1.0, noise=0.002, seed=32)
        df = self._scenario(stock, {"SPY": spy}, {}, sym="UNKNOWNCO")
        # No meta row → source 'no_meta' → rank1 falls to SPY.
        assert not df.empty
        r1 = df[df["rank"] == 1].iloc[0]
        assert r1["hedge_etf"] == "SPY"
        assert r1["classification_source"] == "no_meta"
        assert r1["selection_basis"] == "spy_fallback"

    def test_schema_columns_and_rank_ordering(self):
        smh, smh_rets = _make_etf(self.END, seed=41)
        xlk, _ = _make_etf(self.END, seed=42)
        spy, _ = _make_etf(self.END, seed=43)
        stock = _make_stock_corr(smh_rets, self.END, beta=1.1, noise=0.002, seed=44)
        df = self._scenario(stock, {"SMH": smh, "XLK": xlk, "SPY": spy},
                            {"sector": "Technology", "industry": "Semiconductors"})
        assert list(df.columns) == HEDGE_MAP_COLUMNS
        ranks = sorted(df["rank"].tolist())
        assert ranks == [1, 2, 3]
        assert df[df["rank"] == 3].iloc[0]["hedge_etf"] == "SPY"
        # No non-finite betas emitted.
        assert df["beta"].apply(lambda b: np.isfinite(b) or pd.isna(b)).all()

    def test_insufficient_history_skips_stock(self):
        # Stock with < MIN_N_OBS overlapping obs → no rows.
        spy, spy_rets = _make_etf(self.END, seed=51, n=MIN_N_OBS - 10)
        stock = _make_stock_corr(spy_rets, self.END, beta=1.0, noise=0.002, seed=52)
        df = self._scenario(stock, {"SPY": spy}, {"sector": "Technology", "industry": "Semiconductors"})
        assert df.empty

    def test_unclassified_rank2_basis_is_spy_fallback(self):
        # No sector → rank2 ETF is SPY and its basis must read spy_fallback,
        # not the misleading heuristic_sector.
        spy, spy_rets = _make_etf(self.END, seed=61)
        stock = _make_stock_corr(spy_rets, self.END, beta=1.0, noise=0.002, seed=62)
        df = self._scenario(stock, {"SPY": spy}, {}, sym="NOCLASS")
        r2 = df[df["rank"] == 2].iloc[0]
        assert r2["hedge_etf"] == "SPY"
        assert r2["selection_basis"] == "spy_fallback"

    def test_sector_fallback_rank1_equals_rank2(self):
        # An industry with no pure-play (but a known sector) → rank1 and rank2
        # are both the sector ETF (documented behavior; apex reads rank1).
        xlf, xlf_rets = _make_etf(self.END, seed=71)
        spy, _ = _make_etf(self.END, seed=72)
        stock = _make_stock_corr(xlf_rets, self.END, beta=1.0, noise=0.002, seed=73)
        df = self._scenario(stock, {"XLF": xlf, "SPY": spy},
                            {"sector": "Financial Services", "industry": "Credit Services"})
        r1 = df[df["rank"] == 1].iloc[0]
        r2 = df[df["rank"] == 2].iloc[0]
        assert r1["industry_source"] == "sector_fallback"
        assert r1["hedge_etf"] == "XLF" == r2["hedge_etf"]
        assert r1["selection_basis"] == "heuristic_sector"

    def test_industry_etf_missing_bars_demotes(self):
        # Crosswalk assigns SMH but SMH has NO bars → industry tier fails, demote
        # to the sector ETF (XLK) which does have bars.
        xlk, xlk_rets = _make_etf(self.END, seed=81)
        spy, _ = _make_etf(self.END, seed=82)
        stock = _make_stock_corr(xlk_rets, self.END, beta=1.0, noise=0.002, seed=83)
        # SMH intentionally absent from the bar dict.
        df = self._scenario(stock, {"XLK": xlk, "SPY": spy},
                            {"sector": "Technology", "industry": "Semiconductors"})
        r1 = df[df["rank"] == 1].iloc[0]
        assert r1["hedge_etf"] in ("XLK", "SPY")
        assert np.isfinite(r1["beta"])


class TestSicFallbackClassification(object):
    def test_sic_industry_can_still_map_pure_play(self):
        # When Tiingo sector is null but sicIndustry happens to contain a curated
        # string, the industry crosswalk can still fire. (sicSector usually does
        # NOT map and yields SPY at the sector tier — documented.)
        from hedge_crosswalk import classify
        r = resolve_classification(
            {"sector": None, "industry": None,
             "sicSector": "Mining", "sicIndustry": "Oil & Gas E&P"}
        )
        assert r["source"] == "sic"
        c = classify(r["sector"], r["industry"])
        # sicSector 'Mining' is unmapped → sector tier SPY; industry maps → XOP.
        assert c["industry_etf"] == "XOP"
        assert c["sector_etf"] is None  # Mining not in the GICS SPDR map


class TestCoverageStats:
    def test_coverage_stats_shape(self):
        # Build a tiny hedge map by hand exercising the distinct unclassified
        # buckets (no_meta / isactive_dropped / name_mismatch).
        rows = []
        for sym, src, basis, cls in [
            ("A", "pure_play", "heuristic_industry", "tiingo"),
            ("B", "sector_fallback", "heuristic_sector", "tiingo"),
            ("C", "sector_fallback", "spy_fallback", "no_meta"),
            ("D", "sector_fallback", "spy_fallback", "isactive_dropped"),
            ("E", "sector_fallback", "spy_fallback", "name_mismatch"),
        ]:
            rows.append({"rank": 1, "ticker": sym, "industry_source": src,
                         "selection_basis": basis, "classification_source": cls})
        hm = pd.DataFrame(rows)
        stats = compute_coverage_stats(
            hm, eligible_count=10,
            drop_reasons={"D": "isactive_dropped", "E": "name_mismatch", "Z": "isactive_dropped"},
            eligible_syms=["A", "B", "C", "D", "E"],  # Z not eligible → excluded from guard_drops
        )
        assert stats["covered_tickers"] == 5
        assert stats["industry_source"]["pure_play"] == 1
        assert stats["industry_source"]["sector_fallback"] == 4
        assert stats["selection_basis"]["spy_fallback"] == 3
        assert stats["classification_source"]["no_meta"] == 1
        assert stats["classification_source"]["isactive_dropped"] == 1
        assert stats["classification_source"]["name_mismatch"] == 1
        # guard_drops counts over the ELIGIBLE universe only (Z excluded).
        assert stats["guard_drops"]["isactive_dropped"] == 1
        assert stats["guard_drops"]["name_mismatch"] == 1
        assert stats["coverage_pct_of_eligible"] == 50.0


# ---------------------------------------------------------------------------
# Ticker-reuse guard (isActive + name reconciliation)
# ---------------------------------------------------------------------------

# Real reuse cases verified against the live Tiingo meta endpoint.
_U_ROWS = [
    {"ticker": "u", "isActive": True, "name": "Unity Software Inc",
     "sector": "Technology", "industry": "Software - Application"},
    {"ticker": "u", "isActive": False, "name": "US AIRWAYS GROUP INC",
     "sector": "Industrials", "industry": "Airlines"},
]
_SNOW_ROWS = [
    {"ticker": "snow", "isActive": True, "name": "Snowflake Inc - Class A",
     "sector": "Technology", "industry": "Software - Application"},
    {"ticker": "snow", "isActive": False, "name": "Intrawest Resorts Holdings Inc",
     "sector": "Consumer Cyclical", "industry": "Leisure"},
]


class TestNamesReconcile:
    def test_same_company_minor_punctuation(self):
        assert names_reconcile("Unity Software Inc", "Unity Software Inc.")
        assert names_reconcile("Snowflake Inc - Class A", "Snowflake Inc.")
        assert names_reconcile("Airbnb Inc - Class A", "Airbnb, Inc. Class A Common Stock")

    def test_different_company_rejected(self):
        assert not names_reconcile("US AIRWAYS GROUP INC", "Unity Software Inc.")
        assert not names_reconcile("Intrawest Resorts Holdings Inc", "Snowflake Inc.")

    def test_empty_names(self):
        assert not names_reconcile(None, "Unity Software Inc.")
        assert not names_reconcile("Unity", None)
        assert not names_reconcile("", "")

    def test_suffix_only_names_do_not_falsely_match(self):
        # Two firms sharing only corporate noise tokens must NOT reconcile.
        assert not names_reconcile("Holdings Inc", "Group Corp")


class TestSelectActiveMetaRow:
    def test_picks_active_over_delisted_with_name(self):
        row, reason = select_active_meta_row(_U_ROWS, "Unity Software Inc.")
        assert row["name"] == "Unity Software Inc" and reason == "ok"
        row, reason = select_active_meta_row(_SNOW_ROWS, "Snowflake Inc.")
        assert row["name"] == "Snowflake Inc - Class A" and reason == "ok"

    def test_inactive_only_is_isactive_dropped(self):
        rows = [{"ticker": "x", "isActive": False, "name": "Dead Co",
                 "sector": "Energy", "industry": "Oil & Gas E&P"}]
        row, reason = select_active_meta_row(rows, "Whatever Inc")
        assert row is None and reason == "isactive_dropped"

    def test_active_but_name_mismatch(self):
        rows = [{"ticker": "y", "isActive": True, "name": "Totally Different Corp",
                 "sector": "Energy", "industry": "Oil & Gas E&P"}]
        row, reason = select_active_meta_row(rows, "Unity Software Inc")
        assert row is None and reason == "name_mismatch"

    def test_single_active_no_name_anchor_is_trusted(self):
        rows = [{"ticker": "z", "isActive": True, "name": "Some Co",
                 "sector": "Technology", "industry": "Semiconductors"}]
        row, reason = select_active_meta_row(rows, None)
        assert row["name"] == "Some Co" and reason == "ok"

    def test_multiple_active_no_name_anchor_takes_most_recent(self):
        rows = [
            {"ticker": "z", "isActive": True, "name": "Old", "statementLastUpdated": "2020-01-01"},
            {"ticker": "z", "isActive": True, "name": "New", "statementLastUpdated": "2026-01-01"},
        ]
        row, reason = select_active_meta_row(rows, None)
        assert row["name"] == "New" and reason == "ok"

    def test_empty_rows_is_no_meta(self):
        row, reason = select_active_meta_row([], "Anything")
        assert row is None and reason == "no_meta"


class TestLoadClassificationGuard:
    """load_classification end-to-end with a stubbed S3 client."""

    class _StubS3:
        def __init__(self, rows):
            self._rows = rows

        def get_paginator(self, _op):
            class _P:
                def paginate(self, **_kw):
                    return [{"Contents": [
                        {"Key": "tiingo/json/fundamentals/meta/date=2026-06-26/meta.json"}
                    ]}]
            return _P()

        def get_object(self, **_kw):
            import io as _io
            import json as _json
            return {"Body": _io.BytesIO(_json.dumps(self._rows).encode())}

    def test_reused_ticker_resolves_to_active_company(self):
        from hedge_classification import load_classification
        s3 = self._StubS3(_U_ROWS + _SNOW_ROWS)
        names = {"U": "Unity Software Inc.", "SNOW": "Snowflake Inc."}
        lookup, snap, drops = load_classification(s3, alpaca_names=names)
        assert lookup["U"]["industry"] == "Software - Application"   # not Airlines
        assert lookup["SNOW"]["industry"] == "Software - Application"  # not Leisure
        assert snap == date(2026, 6, 26)
        assert drops == {}  # both resolved cleanly

    def test_reused_ticker_without_names_drops_ambiguous(self):
        from hedge_classification import load_classification
        s3 = self._StubS3(_U_ROWS)  # active Unity + inactive US Airways
        lookup, _, drops = load_classification(s3, alpaca_names=None)
        # Active row is unambiguous (only one active) → trusted even w/o name.
        assert lookup["U"]["industry"] == "Software - Application"
        assert drops == {}

    def test_name_mismatch_recorded_in_drop_reasons(self):
        from hedge_classification import load_classification
        # U's active row is Unity, but we feed a wrong Alpaca name → name_mismatch.
        s3 = self._StubS3(_U_ROWS)
        lookup, _, drops = load_classification(s3, alpaca_names={"U": "Some Other Company"})
        assert "U" not in lookup
        assert drops["U"] == "name_mismatch"


class TestMetaBatching:
    def test_batches_and_concatenates(self, monkeypatch):
        import tiingo_meta_universe as tmu
        seen_batches = []

        class _Resp:
            status_code = 200

            def __init__(self, payload):
                self._p = payload

            def raise_for_status(self):
                pass

            def json(self):
                return self._p

        def _fake_get(url, headers=None, params=None, timeout=None):
            syms = params["tickers"].split(",")
            seen_batches.append(syms)
            # echo one row per ticker
            return _Resp([{"ticker": s, "isActive": True, "name": s} for s in syms])

        monkeypatch.setattr(tmu.requests, "get", _fake_get)
        tickers = [f"T{i}" for i in range(600)]
        rows = tmu.fetch_meta_batched("tok", tickers, batch_size=250, sleep_s=0)
        # 600 / 250 -> 3 batches; all tickers covered.
        assert len(seen_batches) == 3
        assert sum(len(b) for b in seen_batches) == 600
        assert len(rows) == 600
        # tickers were lower-cased for Tiingo.
        assert seen_batches[0][0] == "t0"

    def test_failed_batch_retries_by_splitting(self, monkeypatch):
        # A batch that 502s on its FULL size but succeeds once split should
        # recover via the half-size retry rather than dropping all its tickers.
        import tiingo_meta_universe as tmu

        class _Resp:
            def __init__(self, payload):
                self._p = payload

            def raise_for_status(self):
                pass

            def json(self):
                return self._p

        def _fake_get(url, headers=None, params=None, timeout=None):
            syms = params["tickers"].split(",")
            # Fail only when the batch is "too big" (> 200), like the real 502.
            if len(syms) > 200:
                raise RuntimeError("502 Bad Gateway")
            return _Resp([{"ticker": s} for s in syms])

        monkeypatch.setattr(tmu.requests, "get", _fake_get)
        tickers = [f"T{i}" for i in range(250)]  # one 250 batch → fails → splits to 125+125
        rows = tmu.fetch_meta_batched("tok", tickers, batch_size=250, sleep_s=0)
        assert len(rows) == 250  # both halves recovered

    def test_persistent_single_ticker_failure_is_dropped(self, monkeypatch):
        import tiingo_meta_universe as tmu

        def _fake_get(url, headers=None, params=None, timeout=None):
            raise RuntimeError("always down")

        monkeypatch.setattr(tmu.requests, "get", _fake_get)
        rows = tmu.fetch_meta_batched("tok", ["A", "B"], batch_size=2, sleep_s=0)
        assert rows == []  # split to singles, both fail, dropped (no crash)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
