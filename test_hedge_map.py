"""
Unit tests for hedge_map_flow.py

Tests:
  1. beta_r2_pair parity with lib.py::beta_r2
  2. Eligibility filter (≥90 calendar days)
  3. Liquid + shortable selection screen
  4. Schema conformance
  5. Offline dry-run on synthetic fixture (no network)
"""

from __future__ import annotations

import sys
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

# Allow import from the worktree root.
sys.path.insert(0, os.path.dirname(__file__))

from hedge_map_flow import (
    ETF_CANDIDATES,
    MIN_N_OBS,
    ADV_MIN_USD,
    beta_r2_pair,
    compute_adv,
    compute_eligibility,
    compute_hedge_map,
    _compute_returns,
    _next_trading_day,
    SYMBOL_REMAP,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_price_df(
    n_days: int = 90,
    start: date = date(2024, 1, 2),
    base_price: float = 100.0,
    daily_ret_mean: float = 0.0,
    daily_ret_std: float = 0.01,
    seed: int = 42,
) -> pd.DataFrame:
    """Synthetic daily bars DataFrame (d, close, volume)."""
    rng = np.random.default_rng(seed)
    rets = rng.normal(daily_ret_mean, daily_ret_std, n_days)
    closes = [base_price]
    for r in rets:
        closes.append(closes[-1] * (1 + r))
    closes = closes[1:]  # drop seed price

    # Generate Mon–Fri dates.
    days: list[date] = []
    d = start
    while len(days) < n_days:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    return pd.DataFrame({
        "d": [str(x) for x in days],
        "close": closes,
        "volume": [1_000_000] * n_days,
    })


def _make_correlated_df(
    ref_df: pd.DataFrame,
    beta: float = 1.5,
    noise_std: float = 0.005,
    seed: int = 99,
) -> pd.DataFrame:
    """
    Make a price series correlated to ref_df with known beta.

    Returns a DataFrame with the same dates as ref_df (same length),
    constructed directly from synthetic returns without going through
    _compute_returns so there's no off-by-one from pct_change.
    """
    rng = np.random.default_rng(seed)
    ref_ret = _compute_returns(ref_df)
    ref_rets = ref_ret["ret"].values  # length = len(ref_df) - 1

    stock_rets = beta * ref_rets + rng.normal(0, noise_std, len(ref_rets))

    # Rebuild prices: start from index 1 of ref_df dates (skip first date which
    # has no return). This gives us the same dates as _compute_returns(ref_df).
    dates = ref_ret["d"].tolist()  # date objects, length = len(ref_rets)
    base = 50.0
    closes = [base]
    for r in stock_rets:
        closes.append(closes[-1] * (1 + r))
    closes = closes[1:]  # one close per return, same length as dates

    return pd.DataFrame({
        "d": [str(x) for x in dates],
        "close": closes,
        "volume": [500_000] * len(closes),
    })


# ---------------------------------------------------------------------------
# 1. beta_r2_pair parity with lib.py::beta_r2
# ---------------------------------------------------------------------------

class TestBetaR2Parity:
    """Verify beta_r2_pair matches lib.py::beta_r2 contract."""

    def test_known_beta(self):
        """With synthetic data of known beta, recovered beta should be close."""
        etf_df = _make_price_df(n_days=100, seed=1)
        stock_df = _make_correlated_df(etf_df, beta=1.5, noise_std=0.002, seed=2)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)

        last_date = etf_ret["d"].max()  # already a date object from _compute_returns
        as_of = last_date + timedelta(days=1)
        beta, r2, n_obs = beta_r2_pair(stock_ret, etf_ret, as_of, lookback=60)

        assert np.isfinite(beta), "beta should be finite"
        assert np.isfinite(r2), "r2 should be finite"
        assert 0.0 <= r2 <= 1.0, f"r2 must be in [0,1], got {r2}"
        assert abs(beta - 1.5) < 0.3, f"beta={beta:.3f} should be near 1.5"
        assert n_obs <= 60, "n_obs capped at lookback"

    def test_r2_is_corr_squared(self):
        """r2 must equal corr² (pearson)."""
        etf_df = _make_price_df(n_days=80, seed=10)
        stock_df = _make_correlated_df(etf_df, beta=0.8, noise_std=0.003, seed=11)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)
        as_of = etf_ret["d"].max() + timedelta(days=1)

        beta, r2, n_obs = beta_r2_pair(stock_ret, etf_ret, as_of)
        j = pd.merge(
            stock_ret[["d", "ret"]],
            etf_ret[["d", "ret"]],
            on="d",
            suffixes=("_s", "_e"),
        ).dropna().tail(60)
        expected_r2 = np.corrcoef(j["ret_s"], j["ret_e"])[0, 1] ** 2
        assert abs(r2 - expected_r2) < 1e-10, f"r2={r2} vs expected={expected_r2}"

    def test_no_lookahead(self):
        """Returns ending exactly at as_of - 1 (no look-ahead)."""
        etf_df = _make_price_df(n_days=80, seed=20)
        stock_df = _make_correlated_df(etf_df, beta=1.0, noise_std=0.001, seed=21)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)

        # Set as_of to the date that IS in the data — it should be excluded.
        last_date = etf_ret["d"].max()  # already date object
        as_of = last_date  # as_of = last bar date; that date must be excluded

        _, _, n1 = beta_r2_pair(stock_ret, etf_ret, as_of)

        as_of_next = last_date + timedelta(days=3)  # well beyond the data
        _, _, n2 = beta_r2_pair(stock_ret, etf_ret, as_of_next)

        # With as_of = last date, the last date's return is excluded → fewer obs.
        assert n1 <= n2, f"as_of day must be excluded: n1={n1}, n2={n2}"

    def test_insufficient_data_returns_nan(self):
        """With fewer than 30 overlapping obs, returns (nan, nan, n)."""
        etf_df = _make_price_df(n_days=20, seed=30)
        stock_df = _make_correlated_df(etf_df, beta=1.0, noise_std=0.001, seed=31)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)
        as_of = etf_ret["d"].max() + timedelta(days=1)

        beta, r2, n_obs = beta_r2_pair(stock_ret, etf_ret, as_of)
        assert np.isnan(beta), "beta should be NaN with <30 obs"
        assert np.isnan(r2), "r2 should be NaN with <30 obs"

    def test_n_obs_capped_at_lookback(self):
        """n_obs must not exceed the lookback window."""
        etf_df = _make_price_df(n_days=200, seed=40)
        stock_df = _make_correlated_df(etf_df, beta=1.0, noise_std=0.002, seed=41)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)
        as_of = etf_ret["d"].max() + timedelta(days=1)

        _, _, n_obs = beta_r2_pair(stock_ret, etf_ret, as_of, lookback=60)
        assert n_obs <= 60

    def test_matches_lib_py_formula(self):
        """Cross-check against an independent reimplementation of lib.py::beta_r2."""
        etf_df = _make_price_df(n_days=100, seed=50)
        stock_df = _make_correlated_df(etf_df, beta=1.2, noise_std=0.003, seed=51)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)
        as_of = etf_ret["d"].max() + timedelta(days=1)

        beta_got, r2_got, n_got = beta_r2_pair(stock_ret, etf_ret, as_of)

        # Replicate lib.py formula independently.
        end = (pd.Timestamp(as_of) - pd.Timedelta(days=1)).date()
        s = stock_ret[stock_ret["d"] <= end].tail(60)
        e = etf_ret[etf_ret["d"] <= end].tail(60)
        j = pd.merge(s[["d", "ret"]], e[["d", "ret"]], on="d", suffixes=("_s", "_e")).dropna()
        cov = np.cov(j["ret_s"], j["ret_e"])[0, 1]
        beta_exp = cov / np.var(j["ret_e"], ddof=1)
        r_exp = np.corrcoef(j["ret_s"], j["ret_e"])[0, 1]
        r2_exp = r_exp * r_exp

        assert abs(beta_got - beta_exp) < 1e-12
        assert abs(r2_got - r2_exp) < 1e-12
        assert n_got == len(j)


# ---------------------------------------------------------------------------
# 2. Eligibility filter
# ---------------------------------------------------------------------------

class TestEligibility:
    def _make_universe(self, symbols: list[str]) -> pd.DataFrame:
        return pd.DataFrame({
            "symbol": symbols,
            "shortable": [True] * len(symbols),
            "easy_to_borrow": [True] * len(symbols),
        })

    def test_old_enough_is_eligible(self):
        """Stock with first bar ≥ 90 days before as_of is eligible."""
        as_of = date(2024, 6, 1)
        uni = self._make_universe(["AAPL"])
        # 95 calendar days before as_of
        start = as_of - timedelta(days=95)
        bars = {"AAPL": _make_price_df(n_days=70, start=start)}

        elig = compute_eligibility(uni, bars, as_of)
        assert elig.loc[elig["symbol"] == "AAPL", "eligible"].iloc[0]

    def test_too_new_is_ineligible(self):
        """Stock with first bar only 30 days before as_of is ineligible."""
        as_of = date(2024, 6, 1)
        uni = self._make_universe(["NEWCO"])
        start = as_of - timedelta(days=30)
        bars = {"NEWCO": _make_price_df(n_days=20, start=start)}

        elig = compute_eligibility(uni, bars, as_of)
        assert not elig.loc[elig["symbol"] == "NEWCO", "eligible"].iloc[0]

    def test_no_bars_is_ineligible(self):
        """Symbol with no bars is ineligible."""
        as_of = date(2024, 6, 1)
        uni = self._make_universe(["GHOST"])
        bars: dict = {}

        elig = compute_eligibility(uni, bars, as_of)
        assert not elig.loc[elig["symbol"] == "GHOST", "eligible"].iloc[0]

    def test_exactly_90_days_is_eligible(self):
        """Stock with first bar exactly 90 calendar days before as_of: eligible."""
        as_of = date(2024, 6, 1)
        uni = self._make_universe(["BORDERLINE"])
        start = as_of - timedelta(days=90)  # exactly 90 calendar days before
        # Make sure the first bar IS on `start` (Mon–Fri). If start is a weekend,
        # the first bar will be after start, making it ineligible. To guarantee
        # a first bar on `start`, use a weekday.
        # 2024-06-01 - 90d = 2024-03-03 (Sunday). Skip to 2024-03-04 (Monday).
        while start.weekday() >= 5:
            start += timedelta(days=1)
        bars = {"BORDERLINE": _make_price_df(n_days=60, start=start)}
        # Re-derive as_of as start + 90d to guarantee the boundary.
        as_of_adj = start + timedelta(days=90)
        elig = compute_eligibility(uni, bars, as_of_adj)
        assert elig.loc[elig["symbol"] == "BORDERLINE", "eligible"].iloc[0], \
            f"Expected eligible: first_bar={start}, as_of={as_of_adj}, cutoff={as_of_adj - timedelta(days=90)}"


# ---------------------------------------------------------------------------
# 3. Liquid + shortable selection screen
# ---------------------------------------------------------------------------

class TestSelectionScreen:
    def _make_eligible_universe(self, sym: str) -> pd.DataFrame:
        as_of = date(2024, 6, 1)
        return pd.DataFrame({
            "symbol": [sym],
            "shortable": [True],
            "easy_to_borrow": [True],
            "eligible": [True],
            "first_bar_date": [as_of - timedelta(days=100)],
        })

    def test_illiquid_etf_excluded(self):
        """ETF with ADV < $25M must not appear in the hedge map."""
        as_of = date(2024, 6, 1)
        # All ETFs: price=10, volume=1 → ADV ~ $10, far below $25M
        illiquid_bars: dict[str, pd.DataFrame] = {}
        for etf in ETF_CANDIDATES:
            df = _make_price_df(n_days=80, start=as_of - timedelta(days=95),
                                base_price=10.0, seed=hash(etf) % 1000)
            df["volume"] = 1
            illiquid_bars[etf] = df

        stock_df = _make_correlated_df(illiquid_bars["SPY"], beta=1.0, noise_std=0.001, seed=5)
        stock_df["d"] = illiquid_bars["SPY"]["d"].tail(len(stock_df)).tolist()
        illiquid_bars["AAPL"] = stock_df

        etf_meta = {e: {"shortable": True, "easy_to_borrow": True} for e in ETF_CANDIDATES}
        elig = self._make_eligible_universe("AAPL")

        hm = compute_hedge_map(elig, illiquid_bars, etf_meta, as_of)
        assert hm.empty, "Illiquid ETFs should produce no hedge rows"

    def test_not_shortable_etf_excluded(self):
        """ETF that is not shortable at as_of must not appear in the hedge map."""
        as_of = date(2024, 6, 1)
        etf_df = _make_price_df(n_days=80, start=as_of - timedelta(days=95), base_price=400.0)
        etf_df["volume"] = 1_000_000  # liquid

        stock_df = _make_correlated_df(etf_df, beta=1.0, noise_std=0.001, seed=6)
        all_bars: dict[str, pd.DataFrame] = {}
        for e in ETF_CANDIDATES:
            df = etf_df.copy()
            all_bars[e] = df
        all_bars["AAPL"] = stock_df

        # Mark all ETFs as NOT shortable.
        etf_meta = {e: {"shortable": False, "easy_to_borrow": False} for e in ETF_CANDIDATES}

        elig = self._make_eligible_universe("AAPL")
        hm = compute_hedge_map(elig, all_bars, etf_meta, as_of)
        assert hm.empty, "Non-shortable ETF should produce no hedge rows"

    def test_liquid_shortable_etf_selected(self):
        """ETF that is liquid AND shortable appears in the hedge map."""
        as_of = date(2024, 6, 1)
        start = as_of - timedelta(days=95)

        # SPY: $400 * 10M vol = $4B ADV — clearly liquid.
        etf_df = _make_price_df(n_days=80, start=start, base_price=400.0, seed=100)
        etf_df["volume"] = 10_000_000

        stock_df = _make_correlated_df(etf_df, beta=1.2, noise_std=0.002, seed=101)

        all_bars: dict[str, pd.DataFrame] = {"SPY": etf_df}
        for e in ETF_CANDIDATES:
            if e != "SPY":
                all_bars[e] = pd.DataFrame(columns=["d", "close", "volume"])
        all_bars["TEST"] = stock_df

        etf_meta = {e: {"shortable": e == "SPY", "easy_to_borrow": e == "SPY"}
                    for e in ETF_CANDIDATES}

        elig = pd.DataFrame({
            "symbol": ["TEST"],
            "shortable": [True],
            "easy_to_borrow": [True],
            "eligible": [True],
            "first_bar_date": [start],
        })

        hm = compute_hedge_map(elig, all_bars, etf_meta, as_of)
        assert not hm.empty, "Liquid+shortable ETF should produce hedge rows"
        assert "SPY" in hm["hedge_etf"].values, "SPY should be selected as hedge"


# ---------------------------------------------------------------------------
# 4. Schema conformance
# ---------------------------------------------------------------------------

class TestSchemaConformance:
    REQUIRED_COLS = [
        "effective_date", "as_of_date", "ticker", "rank", "hedge_etf",
        "beta", "r2", "n_obs", "etf_shortable", "etf_easy_to_borrow",
        "etf_adv_usd_30d", "stock_adv_usd_30d", "selection_basis",
    ]

    def _build_minimal_hedge_map(self) -> pd.DataFrame:
        as_of = date(2024, 6, 1)
        start = as_of - timedelta(days=95)

        # SPY: liquid, shortable.
        etf_df = _make_price_df(n_days=80, start=start, base_price=400.0, seed=200)
        etf_df["volume"] = 10_000_000

        stock_df = _make_correlated_df(etf_df, beta=1.3, noise_std=0.002, seed=201)

        all_bars: dict[str, pd.DataFrame] = {"SPY": etf_df, "MYSTOCK": stock_df}
        for e in ETF_CANDIDATES:
            all_bars.setdefault(e, pd.DataFrame(columns=["d", "close", "volume"]))

        etf_meta = {e: {"shortable": e == "SPY", "easy_to_borrow": e == "SPY"}
                    for e in ETF_CANDIDATES}

        elig = pd.DataFrame({
            "symbol": ["MYSTOCK"],
            "shortable": [True],
            "easy_to_borrow": [True],
            "eligible": [True],
            "first_bar_date": [start],
        })

        return compute_hedge_map(elig, all_bars, etf_meta, as_of)

    def test_all_required_columns_present(self):
        hm = self._build_minimal_hedge_map()
        assert not hm.empty, "hedge map should not be empty"
        for col in self.REQUIRED_COLS:
            assert col in hm.columns, f"Missing column: {col}"

    def test_rank_values(self):
        hm = self._build_minimal_hedge_map()
        if not hm.empty:
            assert hm["rank"].between(1, 3).all(), "rank must be 1–3"

    def test_r2_bounds(self):
        hm = self._build_minimal_hedge_map()
        if not hm.empty:
            assert (hm["r2"] >= 0).all() and (hm["r2"] <= 1).all()

    def test_n_obs_lte_lookback(self):
        hm = self._build_minimal_hedge_map()
        if not hm.empty:
            assert (hm["n_obs"] <= 60).all()

    def test_selection_basis_value(self):
        hm = self._build_minimal_hedge_map()
        if not hm.empty:
            assert (hm["selection_basis"] == "liquid_top_r2").all()

    def test_etf_shortable_is_bool(self):
        hm = self._build_minimal_hedge_map()
        if not hm.empty:
            assert hm["etf_shortable"].dtype == bool or hm["etf_shortable"].dtype == object


# ---------------------------------------------------------------------------
# 5. Offline dry-run on synthetic fixture universe
# ---------------------------------------------------------------------------

class TestOfflineDryRun:
    """Full pipeline run on a tiny synthetic fixture universe — no network calls."""

    def _build_fixture_bars(
        self,
        n_stocks: int = 5,
        n_etfs: int = 3,
        as_of: date = date(2024, 6, 1),
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, dict]]:
        """Build a minimal fixture: a few stocks, a few ETFs from the candidate list."""
        start = as_of - timedelta(days=100)
        stocks = [f"STOCK{i}" for i in range(n_stocks)]
        etfs_to_use = ETF_CANDIDATES[:n_etfs]  # SPY, VOO, VTI

        all_bars: dict[str, pd.DataFrame] = {}
        # ETF bars: liquid enough ($400 * 5M = $2B ADV).
        for etf in etfs_to_use:
            df = _make_price_df(n_days=85, start=start, base_price=400.0,
                                seed=hash(etf) % 1000)
            df["volume"] = 5_000_000
            all_bars[etf] = df

        # Fill remaining ETF candidates with empty bars.
        for e in ETF_CANDIDATES:
            all_bars.setdefault(e, pd.DataFrame(columns=["d", "close", "volume"]))

        # Stock bars: correlated to SPY with known betas.
        spy_df = all_bars["SPY"]
        for i, sym in enumerate(stocks):
            corr_df = _make_correlated_df(spy_df, beta=0.8 + i * 0.1, noise_std=0.003,
                                          seed=200 + i)
            all_bars[sym] = corr_df

        universe = pd.DataFrame({
            "symbol": stocks,
            "shortable": [True] * n_stocks,
            "easy_to_borrow": [True] * n_stocks,
        })

        etf_meta = {e: {"shortable": True, "easy_to_borrow": True}
                    for e in ETF_CANDIDATES}

        return universe, all_bars, etf_meta

    def test_dry_run_produces_output(self):
        """Full pipeline on fixture: should produce hedge map rows."""
        as_of = date(2024, 6, 1)
        universe, all_bars, etf_meta = self._build_fixture_bars(n_stocks=5, n_etfs=3, as_of=as_of)

        elig = compute_eligibility(universe, all_bars, as_of)
        assert elig["eligible"].sum() == 5, "All 5 stocks should be eligible"

        hm = compute_hedge_map(elig, all_bars, etf_meta, as_of)
        assert not hm.empty, "Hedge map must not be empty"
        assert len(hm) >= 5, "At least one row per stock"

    def test_effective_date_is_next_trading_day(self):
        """effective_date must be the trading day after as_of."""
        as_of = date(2024, 5, 31)  # Friday
        universe, all_bars, etf_meta = self._build_fixture_bars(n_stocks=2, as_of=as_of)
        elig = compute_eligibility(universe, all_bars, as_of)
        hm = compute_hedge_map(elig, all_bars, etf_meta, as_of)
        if not hm.empty:
            eff = pd.to_datetime(hm["effective_date"].iloc[0]).date()
            expected = _next_trading_day(as_of)
            assert eff == expected, f"expected effective_date={expected}, got {eff}"

    def test_no_hedge_with_zero_stocks(self):
        """Empty eligible universe produces empty hedge map."""
        as_of = date(2024, 6, 1)
        empty_elig = pd.DataFrame({
            "symbol": pd.Series([], dtype=str),
            "shortable": pd.Series([], dtype=bool),
            "easy_to_borrow": pd.Series([], dtype=bool),
            "eligible": pd.Series([], dtype=bool),
            "first_bar_date": pd.Series([], dtype=object),
        })
        _, all_bars, etf_meta = self._build_fixture_bars(n_stocks=0, as_of=as_of)
        hm = compute_hedge_map(empty_elig, all_bars, etf_meta, as_of)
        assert hm.empty, "Empty eligible universe must yield empty hedge map"

    def test_top3_rank_ordering(self):
        """For a given ticker, rank=1 must have >= r2 of rank=2 etc."""
        as_of = date(2024, 6, 1)
        universe, all_bars, etf_meta = self._build_fixture_bars(n_stocks=3, n_etfs=5, as_of=as_of)
        elig = compute_eligibility(universe, all_bars, as_of)
        hm = compute_hedge_map(elig, all_bars, etf_meta, as_of)

        for ticker in hm["ticker"].unique():
            sub = hm[hm["ticker"] == ticker].sort_values("rank")
            r2_vals = sub["r2"].tolist()
            assert r2_vals == sorted(r2_vals, reverse=True), \
                f"ranks must be ordered by r2 desc for {ticker}"


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_symbol_remap_keys(self):
        """SYMBOL_REMAP must contain the documented canonical remaps."""
        assert SYMBOL_REMAP.get("MOGA") == "MOG.A"
        assert SYMBOL_REMAP.get("BRKB") == "BRK.B"
        assert SYMBOL_REMAP.get("SIVB.Q") == "SIVB"

    def test_etf_candidates_count(self):
        """ETF_CANDIDATES must have exactly 57 entries."""
        assert len(ETF_CANDIDATES) == 57, f"Expected 57, got {len(ETF_CANDIDATES)}"

    def test_next_trading_day_skips_weekend(self):
        """_next_trading_day skips Saturday and Sunday."""
        friday = date(2024, 5, 31)
        assert _next_trading_day(friday) == date(2024, 6, 3), "Friday -> Monday"

        thursday = date(2024, 5, 30)
        assert _next_trading_day(thursday) == date(2024, 5, 31), "Thursday -> Friday"

    def test_compute_returns_pct_change(self):
        """_compute_returns uses pct_change on close prices."""
        df = pd.DataFrame({"d": ["2024-01-02", "2024-01-03", "2024-01-04"],
                           "close": [100.0, 102.0, 99.96],
                           "volume": [1e6, 1e6, 1e6]})
        r = _compute_returns(df)
        assert abs(r.iloc[0]["ret"] - 0.02) < 1e-10
        assert abs(r.iloc[1]["ret"] - (-0.02)) < 1e-6


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
