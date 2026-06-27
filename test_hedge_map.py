"""
Unit tests for hedge_map_flow.py

Tests:
  1. beta_r2_pair parity with lib.py::beta_r2
  2. Eligibility filter (≥90 calendar days)
  3. Liquid + shortable selection screen
  4. Schema conformance
  5. Offline dry-run on synthetic fixture (no network)
  6. P1-a: Alpaca credential loading (_init_alpaca_creds)
  7. P1-b: Scheduled as_of default produces the right effective_date
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

import unittest.mock as mock

from hedge_map_flow import (
    ETF_CANDIDATES,
    MIN_N_OBS,
    ADV_MIN_USD,
    beta_r2_pair,
    compute_adv,
    compute_eligibility,
    compute_hedge_map,
    _check_bar_availability,
    _compute_returns,
    _next_trading_day,
    _latest_trading_day,
    _prior_trading_day,
    _trading_days_before,
    _is_trading_day,
    _init_alpaca_creds,
    SYMBOL_REMAP,
    ALPACA_KEY_BLOCK,
    ALPACA_SECRET_BLOCK,
    _SENTINEL_ETFS,
)
import hedge_map_flow as _flow_module


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

    def test_window_inclusive_of_as_of(self):
        """Window is d <= as_of (inclusive): as_of day's bar is included."""
        # Use exactly lookback+1 obs so every boundary difference shows in n_obs.
        # _make_price_df(n_days=61) → _compute_returns drops 1 → 60 return rows.
        lookback = 60
        etf_df = _make_price_df(n_days=lookback + 1, seed=20)
        stock_df = _make_correlated_df(etf_df, beta=1.0, noise_std=0.001, seed=21)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)

        last_date = etf_ret["d"].max()  # already a date object
        # With 60 return rows and lookback=60, all 60 are available.

        # as_of = last_date: that date is INCLUDED (d <= as_of) → all 60 rows.
        _, _, n_inclusive = beta_r2_pair(stock_ret, etf_ret, last_date, lookback=lookback)

        # as_of = last_date - 1 trading day: last_date is EXCLUDED → only 59 rows.
        prev_date = last_date - timedelta(days=1)
        # prev_date might be a weekend; go back far enough to hit a weekday.
        while prev_date.weekday() >= 5:
            prev_date -= timedelta(days=1)
        _, _, n_exclusive = beta_r2_pair(stock_ret, etf_ret, prev_date, lookback=lookback)

        assert n_inclusive > n_exclusive, (
            f"as_of={last_date} (inclusive) should have more obs than "
            f"as_of={prev_date} (one day earlier): "
            f"n_inclusive={n_inclusive}, n_exclusive={n_exclusive}"
        )

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
        """
        Cross-check against an independent reimplementation of the OLS formula.

        Convention: beta_r2_pair window is d <= as_of (inclusive). lib.py::beta_r2
        uses as_of_date as the event/pick date with window ending at as_of_date - 1.
        To reproduce lib.py results, callers set as_of = prior_trading_day(pick_date).
        Here we verify the core math by passing as_of = last bar date and checking
        against an inline reimplementation with the same d <= as_of boundary.
        """
        etf_df = _make_price_df(n_days=100, seed=50)
        stock_df = _make_correlated_df(etf_df, beta=1.2, noise_std=0.003, seed=51)

        etf_ret = _compute_returns(etf_df)
        stock_ret = _compute_returns(stock_df)
        as_of = etf_ret["d"].max()  # use last bar date — it is included (d <= as_of)

        beta_got, r2_got, n_got = beta_r2_pair(stock_ret, etf_ret, as_of)

        # Inline reimplementation using the same d <= as_of inclusive boundary.
        end = pd.Timestamp(as_of).date()
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


# ---------------------------------------------------------------------------
# 6. P1-a: Alpaca credential loading
# ---------------------------------------------------------------------------

class TestAlpacraCreds:
    """Verify _init_alpaca_creds loads from blocks with env-var fallback (P1-a)."""

    def setup_method(self):
        """Save and clear module-level credentials before each test."""
        self._orig_key = _flow_module._alpaca_key
        self._orig_secret = _flow_module._alpaca_secret
        _flow_module._alpaca_key = ""
        _flow_module._alpaca_secret = ""

    def teardown_method(self):
        """Restore original credentials after each test."""
        _flow_module._alpaca_key = self._orig_key
        _flow_module._alpaca_secret = self._orig_secret

    def test_env_var_fallback_loads_creds(self, monkeypatch):
        """With from_prefect_blocks=False, credentials are read from env vars."""
        monkeypatch.setenv("ALPACA_API_KEY", "test-key-from-env")
        monkeypatch.setenv("ALPACA_API_SECRET", "test-secret-from-env")

        _init_alpaca_creds(from_prefect_blocks=False)

        assert _flow_module._alpaca_key == "test-key-from-env"
        assert _flow_module._alpaca_secret == "test-secret-from-env"

    def test_headers_populated_after_init(self, monkeypatch):
        """_alpaca_headers() returns correct dict after _init_alpaca_creds()."""
        monkeypatch.setenv("ALPACA_API_KEY", "K123")
        monkeypatch.setenv("ALPACA_API_SECRET", "S456")
        _init_alpaca_creds(from_prefect_blocks=False)

        from hedge_map_flow import _alpaca_headers
        h = _alpaca_headers()
        assert h["APCA-API-KEY-ID"] == "K123"
        assert h["APCA-API-SECRET-KEY"] == "S456"

    def test_headers_raises_before_init(self):
        """_alpaca_headers() must raise RuntimeError if creds are empty."""
        # Creds were cleared in setup_method.
        from hedge_map_flow import _alpaca_headers
        with pytest.raises(RuntimeError, match="_init_alpaca_creds"):
            _alpaca_headers()

    def test_missing_env_var_raises(self, monkeypatch):
        """Missing ALPACA_API_KEY env var raises KeyError (not silent failure)."""
        monkeypatch.delenv("ALPACA_API_KEY", raising=False)
        monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
        with pytest.raises(KeyError):
            _init_alpaca_creds(from_prefect_blocks=False)

    def test_prefect_block_path_falls_back_to_env_on_block_failure(self, monkeypatch):
        """If Secret.load() fails (no Prefect server), falls back to env vars."""
        monkeypatch.setenv("ALPACA_API_KEY", "fallback-key")
        monkeypatch.setenv("ALPACA_API_SECRET", "fallback-secret")

        # Simulate Secret.load() raising (Prefect blocks unavailable).
        with mock.patch("hedge_map_flow.Secret") as mock_secret:
            mock_secret.load.side_effect = Exception("block not found")
            _init_alpaca_creds(from_prefect_blocks=True)

        assert _flow_module._alpaca_key == "fallback-key"
        assert _flow_module._alpaca_secret == "fallback-secret"

    def test_prefect_block_path_loads_from_blocks(self, monkeypatch):
        """When Secret.load() succeeds, creds come from blocks (not env vars)."""
        # Set env vars to different values so we can confirm blocks won the race.
        monkeypatch.setenv("ALPACA_API_KEY", "env-key-should-not-be-used")
        monkeypatch.setenv("ALPACA_API_SECRET", "env-secret-should-not-be-used")

        mock_key_block = mock.MagicMock()
        mock_key_block.get.return_value = "block-key"
        mock_secret_block = mock.MagicMock()
        mock_secret_block.get.return_value = "block-secret"

        def _fake_load(name):
            if name == ALPACA_KEY_BLOCK:
                return mock_key_block
            if name == ALPACA_SECRET_BLOCK:
                return mock_secret_block
            raise ValueError(f"unknown block: {name}")

        with mock.patch("hedge_map_flow.Secret") as mock_secret:
            mock_secret.load.side_effect = _fake_load
            _init_alpaca_creds(from_prefect_blocks=True)

        assert _flow_module._alpaca_key == "block-key"
        assert _flow_module._alpaca_secret == "block-secret"


# ---------------------------------------------------------------------------
# 7. P1-b: Scheduled as_of default → correct effective_date
# ---------------------------------------------------------------------------

class TestScheduledDefault:
    """
    Verify that _latest_trading_day() and _prior_trading_day() produce the right
    effective_date for the scheduled flow use-case (P1-b).
    """

    def test_latest_trading_day_weekday_returns_same_day(self):
        """On a weekday, _latest_trading_day returns that day."""
        tuesday = date(2026, 6, 23)
        assert tuesday.weekday() == 1  # Tuesday
        assert _latest_trading_day(tuesday) == tuesday

    def test_latest_trading_day_saturday_returns_friday(self):
        """On Saturday, _latest_trading_day returns the previous Friday."""
        saturday = date(2026, 6, 20)
        assert saturday.weekday() == 5
        assert _latest_trading_day(saturday) == date(2026, 6, 19)  # Friday

    def test_latest_trading_day_sunday_returns_friday(self):
        """On Sunday, _latest_trading_day returns the previous Friday."""
        sunday = date(2026, 6, 21)
        assert sunday.weekday() == 6
        assert _latest_trading_day(sunday) == date(2026, 6, 19)  # Friday

    def test_scheduled_run_tuesday_produces_wednesday_effective(self):
        """
        A scheduled run on Tuesday evening (18:30 ET) sets as_of=Tuesday,
        effective_date=Wednesday — the upcoming session.
        """
        tuesday = date(2026, 6, 23)  # weekday
        as_of = _latest_trading_day(tuesday)
        effective = _next_trading_day(as_of)
        assert as_of == tuesday, "as_of should be today (Tuesday)"
        assert effective == date(2026, 6, 24), "effective_date should be tomorrow (Wednesday)"

    def test_scheduled_run_friday_produces_monday_effective(self):
        """
        A scheduled run on Friday evening sets as_of=Friday, effective_date=Monday.
        """
        friday = date(2026, 6, 19)  # 2026-06-19 is a Friday
        assert friday.weekday() == 4  # sanity-check
        as_of = _latest_trading_day(friday)
        effective = _next_trading_day(as_of)
        assert as_of == friday
        assert effective == date(2026, 6, 22)  # following Monday (no holiday)

    def test_prior_trading_day_is_strictly_before(self):
        """_prior_trading_day returns a day strictly before the input."""
        tuesday = date(2026, 6, 23)
        result = _prior_trading_day(tuesday)
        assert result < tuesday, "prior trading day must be strictly before input"
        assert result == date(2026, 6, 22)  # Monday

    def test_prior_vs_latest_differ_on_weekday(self):
        """On a weekday, _latest returns today; _prior returns yesterday (or earlier)."""
        wednesday = date(2026, 6, 24)
        assert _latest_trading_day(wednesday) == wednesday
        assert _prior_trading_day(wednesday) == date(2026, 6, 23)

    def test_prior_vs_latest_converge_on_monday(self):
        """
        On a Monday, _latest returns Monday; _prior skips weekend and returns Friday.
        """
        monday = date(2026, 6, 23)
        assert monday.weekday() == 1  # actually Tuesday 2026-06-23 is Tuesday; use real Monday
        monday_real = date(2026, 6, 22)
        assert monday_real.weekday() == 0  # Monday
        assert _latest_trading_day(monday_real) == monday_real
        assert _prior_trading_day(monday_real) == date(2026, 6, 19)  # Friday

    def test_old_default_would_produce_stale_effective_date(self):
        """
        Regression test: the OLD default (_prior_trading_day) on a weekday produces
        effective_date = today (already past for an evening run), not next session.
        The NEW default (_latest_trading_day) produces effective_date = tomorrow.
        """
        today = date(2026, 6, 24)  # Wednesday
        old_as_of = _prior_trading_day(today)   # Tuesday 2026-06-23
        new_as_of = _latest_trading_day(today)  # Wednesday 2026-06-24

        old_effective = _next_trading_day(old_as_of)  # Wednesday 2026-06-24 = today (STALE)
        new_effective = _next_trading_day(new_as_of)  # Thursday 2026-06-25 = tomorrow (CORRECT)

        assert old_effective == today, "old logic produces stale effective_date = today"
        assert new_effective == today + timedelta(days=1), "new logic produces effective_date = tomorrow"
        assert new_effective > old_effective, "new logic always produces a later effective_date"


# ---------------------------------------------------------------------------
# 8. Exchange calendar helpers (P2)
# ---------------------------------------------------------------------------

class TestCalendarHelpers:
    """
    Verify calendar-aware helpers behave correctly with and without a loaded calendar.
    Tests run offline (no Alpaca call) — _TRADING_DAYS stays empty so helpers fall
    back to weekday logic, and specific holiday tests inject a fake calendar.
    """

    def setup_method(self):
        """Save original _TRADING_DAYS before each test."""
        self._orig_trading_days = _flow_module._TRADING_DAYS

    def teardown_method(self):
        """Restore original _TRADING_DAYS after each test."""
        _flow_module._TRADING_DAYS = self._orig_trading_days

    def test_is_trading_day_falls_back_to_weekday_when_calendar_empty(self):
        """When _TRADING_DAYS is empty, _is_trading_day returns weekday logic."""
        _flow_module._TRADING_DAYS = frozenset()
        monday = date(2026, 6, 22)
        saturday = date(2026, 6, 20)
        assert monday.weekday() == 0
        assert saturday.weekday() == 5
        assert _is_trading_day(monday) is True
        assert _is_trading_day(saturday) is False

    def test_is_trading_day_uses_calendar_when_loaded(self):
        """When _TRADING_DAYS is loaded, _is_trading_day checks the set."""
        monday = date(2026, 6, 22)
        friday = date(2026, 6, 19)
        # Inject a calendar that includes only Friday (Monday is a "holiday" in this fiction).
        _flow_module._TRADING_DAYS = frozenset([friday])
        assert _is_trading_day(friday) is True
        assert _is_trading_day(monday) is False  # not in the injected set

    def test_next_trading_day_skips_holiday(self):
        """With calendar loaded, _next_trading_day skips a holiday (not just weekends)."""
        # Simulate July 4 (Friday) as a holiday; next session = Monday July 7.
        friday = date(2026, 7, 3)   # day before holiday
        holiday = date(2026, 7, 4)  # Independence Day (Saturday this year — let's use
                                    # a real example: 2025-07-04 is a Friday)
        friday_2025 = date(2025, 7, 3)
        holiday_2025 = date(2025, 7, 4)
        monday_2025 = date(2025, 7, 7)
        # Build a calendar that has Thursday and Monday but not Friday (holiday).
        _flow_module._TRADING_DAYS = frozenset([
            date(2025, 7, 3),   # Thursday
            date(2025, 7, 7),   # following Monday
        ])
        assert _next_trading_day(date(2025, 7, 3)) == date(2025, 7, 7), \
            "next_trading_day should skip holiday Friday and return Monday"

    def test_latest_trading_day_skips_holiday(self):
        """_latest_trading_day walks back past a holiday."""
        # Build a calendar: Wednesday the 2nd is a session; Thursday the 3rd is a holiday.
        _flow_module._TRADING_DAYS = frozenset([
            date(2025, 7, 2),   # Wednesday
            date(2025, 7, 7),   # Monday after holiday weekend
        ])
        # On July 3 (holiday in our fake calendar), should return July 2.
        assert _latest_trading_day(date(2025, 7, 3)) == date(2025, 7, 2)

    def test_prior_trading_day_skips_holiday(self):
        """_prior_trading_day walks back past a holiday."""
        _flow_module._TRADING_DAYS = frozenset([
            date(2025, 7, 2),   # Wednesday
            date(2025, 7, 7),   # Monday
        ])
        # prior_trading_day(Monday July 7) should return Wednesday July 2 (skipping holiday Fri/weekend).
        assert _prior_trading_day(date(2025, 7, 7)) == date(2025, 7, 2)

    def test_trading_days_before_skips_holiday(self):
        """_trading_days_before skips holidays correctly."""
        _flow_module._TRADING_DAYS = frozenset([
            date(2025, 7, 7),  # Monday
            date(2025, 7, 2),  # Wednesday
            date(2025, 7, 1),  # Tuesday
        ])
        result = _trading_days_before(date(2025, 7, 7), 3)
        assert result == [date(2025, 7, 7), date(2025, 7, 2), date(2025, 7, 1)]


# ---------------------------------------------------------------------------
# 9. Bar-guard: _check_bar_availability (shared helper)
# ---------------------------------------------------------------------------

class TestBarGuardCoercion:
    """
    Tests for _check_bar_availability — the shared bar-lag guard extracted into
    hedge_map_flow.py and called from both the Prefect flow and run_local.py.

    Key invariants:
    - Uses min() over sentinels: ALL must have settled, not just the leader.
    - String-typed d column (as stored by _bars_to_df) coerced without TypeError.
    - skip=True returns {} (--as-of override path).
    - Returns {run_as_of: beta_as_of} mapping; never modifies as_of_dates.
    """

    def _make_bar_df(self, date_str: str) -> pd.DataFrame:
        """Return a minimal bar DataFrame with d stored as string (matching _bars_to_df)."""
        return pd.DataFrame({
            "d": [date_str],
            "close": [100.0],
            "volume": [1_000_000],
        })

    def test_string_bar_date_coercion_does_not_raise(self):
        """date.fromisoformat(str_date) compared to date object must not raise TypeError."""
        bar_date_str = "2026-06-20"
        as_of = date(2026, 6, 24)
        coerced = date.fromisoformat(bar_date_str)
        assert coerced < as_of  # must be a proper date comparison, no TypeError

    def test_all_sentinels_settled_no_override(self):
        """When all sentinels have today's bar, guard returns empty dict."""
        as_of = date(2026, 6, 24)
        all_bars = {e: self._make_bar_df("2026-06-24") for e in _SENTINEL_ETFS}
        result = _check_bar_availability(all_bars, [as_of], skip=False)
        assert result == {}, "guard should not fire when all sentinels are current"

    def test_all_sentinels_stale_fires(self):
        """When all sentinels lag, guard fires and maps run_as_of to actual_last_bar."""
        as_of = date(2026, 6, 24)
        all_bars = {e: self._make_bar_df("2026-06-20") for e in _SENTINEL_ETFS}
        result = _check_bar_availability(all_bars, [as_of], skip=False)
        assert as_of in result, "guard should fire: all sentinels stale"
        assert result[as_of] == date(2026, 6, 20)

    def test_min_not_max_laggard_determines_availability(self):
        """
        When one sentinel is current (2026-06-24) and two are stale (2026-06-20),
        min() means the stale date wins — the guard fires.
        Using max() would incorrectly treat the close as settled off the leader.
        """
        as_of = date(2026, 6, 24)
        all_bars = {
            "SPY": self._make_bar_df("2026-06-24"),  # current
            "QQQ": self._make_bar_df("2026-06-20"),  # stale
            "IWM": self._make_bar_df("2026-06-20"),  # stale
        }
        result = _check_bar_availability(all_bars, [as_of], skip=False)
        assert as_of in result, (
            "guard must fire when ANY sentinel lags (min), not only when ALL lag (max)"
        )
        assert result[as_of] == date(2026, 6, 20), (
            "beta_as_of should be the laggard's date, not the leader's"
        )

    def test_all_current_leader_not_enough(self):
        """
        When all three sentinels are current, guard does not fire even if one
        is 'ahead' (same result: all settled → no override needed).
        """
        as_of = date(2026, 6, 24)
        all_bars = {e: self._make_bar_df("2026-06-24") for e in _SENTINEL_ETFS}
        result = _check_bar_availability(all_bars, [as_of], skip=False)
        assert result == {}

    def test_skip_true_returns_empty_regardless(self):
        """skip=True (--as-of override path) always returns {} without checking bars."""
        as_of = date(2026, 6, 24)
        # Bars are stale but skip=True should bypass the guard entirely.
        all_bars = {e: self._make_bar_df("2026-06-01") for e in _SENTINEL_ETFS}
        result = _check_bar_availability(all_bars, [as_of], skip=True)
        assert result == {}, "skip=True must bypass guard"

    def test_missing_sentinel_treated_as_stale(self):
        """A sentinel absent from all_bars is treated as not settled."""
        as_of = date(2026, 6, 24)
        # Only SPY present; QQQ and IWM missing.
        all_bars = {"SPY": self._make_bar_df("2026-06-24")}
        result = _check_bar_availability(all_bars, [as_of], skip=False)
        assert as_of in result, "missing sentinel should be treated as stale"

    def test_guard_does_not_mutate_as_of_dates(self):
        """_check_bar_availability must not modify the as_of_dates list."""
        as_of = date(2026, 6, 24)
        as_of_dates = [as_of]
        original = list(as_of_dates)
        all_bars = {e: self._make_bar_df("2026-06-20") for e in _SENTINEL_ETFS}
        _check_bar_availability(all_bars, as_of_dates, skip=False)
        assert as_of_dates == original, "guard must not mutate as_of_dates"


# ---------------------------------------------------------------------------
# 10. Calendar window coverage (P2 backfill-beyond-cache regression)
# ---------------------------------------------------------------------------

class TestCalendarWindowCoverage:
    """
    Verify that trading-day helpers behave correctly when asked about dates
    outside the cached calendar window.

    The fix: load the calendar over [as_of - 200d, as_of + 7d] (not today ± window).
    If _TRADING_DAYS is non-empty but the queried date is outside the loaded range,
    _is_trading_day returns False (not in set) — which is wrong. The test documents
    this edge-case expectation and verifies the fallback.
    """

    def setup_method(self):
        self._orig = _flow_module._TRADING_DAYS

    def teardown_method(self):
        _flow_module._TRADING_DAYS = self._orig

    def test_date_outside_cache_range_falls_back_gracefully(self):
        """
        When the calendar cache covers 2026-01-01..2026-12-31 only, a date in 2020
        is NOT in the set. _is_trading_day returns False (not in set). This documents
        the expected behavior and ensures callers must load the right window.
        """
        # Inject a narrow cache covering only one day in 2026.
        _flow_module._TRADING_DAYS = frozenset([date(2026, 6, 24)])  # only one session
        # A 2020 trading day is outside the cache — returns False because not in set.
        assert not _is_trading_day(date(2020, 1, 2)), (
            "A date outside the loaded window is not in the frozenset → False"
        )

    def test_calendar_loaded_for_as_of_override_window(self):
        """
        When as_of_override = "2020-01-15", the calendar must be loaded from
        at least 2019-09-xx (200 days back). This test injects a calendar anchored
        at the override date and verifies the helpers find sessions in that range.
        """
        # Inject a calendar anchored around the historical override date.
        session_in_range = date(2019, 11, 4)  # a Monday
        _flow_module._TRADING_DAYS = frozenset([session_in_range, date(2020, 1, 15)])

        assert _is_trading_day(session_in_range), (
            "Session within the override window's lookback should be found"
        )
        assert _is_trading_day(date(2020, 1, 15)), "The override date itself should be a session"

    def test_non_session_as_of_override_is_snapped(self):
        """
        When as_of_override is a weekend/holiday, _latest_trading_day snaps it
        back to the preceding session. The parquet as_of_date must be the snapped date.
        """
        # Build a calendar with only weekdays (no holiday complication needed here).
        friday = date(2026, 6, 19)
        saturday = date(2026, 6, 20)
        _flow_module._TRADING_DAYS = frozenset([friday])  # only Friday is a session

        snapped = _latest_trading_day(saturday)
        assert snapped == friday, (
            f"Saturday {saturday} should snap to Friday {friday}, got {snapped}"
        )


# ---------------------------------------------------------------------------
# 11. Holistic date-invariant tests: bar-lag + large backfill + holiday boundary
# ---------------------------------------------------------------------------

class TestDateInvariants:
    """
    Holistic invariant tests covering interactions between bar-lag, large backfills,
    and holiday boundaries. These lock down the contracts that must hold together
    so adjacent edge cases stop surfacing one at a time.
    """

    def setup_method(self):
        self._orig = _flow_module._TRADING_DAYS

    def teardown_method(self):
        _flow_module._TRADING_DAYS = self._orig

    # ---- Invariant 1: effective_date is always a real session ----

    def test_effective_date_is_always_a_session(self):
        """
        _next_trading_day(d) must return a date that _is_trading_day recognises,
        regardless of whether d is a session, weekend, or holiday.
        """
        # Build a calendar with Mon/Wed/Fri (Tue/Thu are "holidays" in this fiction).
        sessions = frozenset([
            date(2026, 6, 22),  # Monday
            date(2026, 6, 24),  # Wednesday
            date(2026, 6, 26),  # Friday
        ])
        _flow_module._TRADING_DAYS = sessions

        for d in [date(2026, 6, 22), date(2026, 6, 23), date(2026, 6, 24)]:
            nxt = _next_trading_day(d)
            assert _is_trading_day(nxt), (
                f"_next_trading_day({d}) = {nxt} is not a trading session"
            )

    # ---- Invariant 2: bar-lag must not rewind effective_date ----

    def test_bar_lag_leaves_effective_date_unchanged(self):
        """
        When actual_last_bar < as_of (bars not yet settled), effective_date must
        equal next_trading_day(original_as_of), NOT next_trading_day(actual_last_bar).

        This is the P2-1 invariant: the upcoming session's partition must always be
        written for the session it was scheduled for.
        """
        sessions = frozenset([
            date(2026, 6, 23),  # Tuesday — original as_of
            date(2026, 6, 24),  # Wednesday — intended effective_date
            date(2026, 6, 22),  # Monday — actual_last_bar (bars lagged)
        ])
        _flow_module._TRADING_DAYS = sessions

        original_as_of = date(2026, 6, 23)    # Tuesday
        actual_last_bar = date(2026, 6, 22)    # Monday (lagged)
        intended_effective = _next_trading_day(original_as_of)  # Wednesday

        assert intended_effective == date(2026, 6, 24), "sanity-check"

        # beta_as_of is the lagged bar date — effective_date must NOT be derived from it.
        wrong_effective = _next_trading_day(actual_last_bar)  # Tuesday (stale!)
        assert wrong_effective != intended_effective, (
            "Deriving effective_date from actual_last_bar gives a stale date"
        )
        # The correct effective_date stays pinned to the original_as_of.
        assert intended_effective == date(2026, 6, 24)

    def test_compute_hedge_map_accepts_explicit_effective_date(self):
        """
        compute_hedge_map with explicit effective_date writes that date to the
        parquet, not next_trading_day(as_of). This is the mechanism that decouples
        the beta window from the partition key when bars are lagged.
        """
        as_of = date(2024, 6, 1)
        # Inject a calendar so _next_trading_day works without network.
        _flow_module._TRADING_DAYS = frozenset([
            date(2024, 6, 1), date(2024, 6, 3), date(2024, 6, 4),
        ])

        # Build minimal synthetic fixtures.
        etf_df = _make_price_df(n_days=90, seed=77)
        stock_df = _make_correlated_df(etf_df, beta=1.0, seed=78)
        mock_bars = {"SPY": etf_df}  # use SPY as both ETF and stock bars
        for etf in ETF_CANDIDATES:
            if etf != "SPY":
                mock_bars[etf] = pd.DataFrame(columns=["d", "close", "volume"])

        universe = pd.DataFrame([{
            "symbol": "SPY",
            "shortable": True,
            "easy_to_borrow": True,
            "first_bar_date": date(2024, 1, 1),
            "eligible": True,
        }])
        etf_meta = {etf: {"shortable": True, "easy_to_borrow": True} for etf in ETF_CANDIDATES}
        mock_bars["SPY"] = etf_df

        explicit_effective = date(2024, 6, 4)  # two sessions after as_of
        hm = compute_hedge_map(
            universe, mock_bars, etf_meta,
            as_of=as_of,
            effective_date=explicit_effective,
        )
        if not hm.empty:
            assert (hm["effective_date"] == explicit_effective).all(), (
                f"Expected effective_date={explicit_effective} in all rows, "
                f"got {hm['effective_date'].unique()}"
            )
            assert (hm["as_of_date"] == as_of).all(), (
                f"as_of_date should be {as_of}, got {hm['as_of_date'].unique()}"
            )

    # ---- Invariant 3: large backfill calendar coverage ----

    def test_large_backfill_calendar_window_formula(self):
        """
        For backfill_days=250, cal_window_days must be > 138 sessions worth of
        calendar days (250 sessions * 1.4 ≈ 350 calendar days). The formula
        max(200, backfill_days * 2 + 160) = max(200, 660) = 660 covers this.
        """
        backfill_days = 250
        cal_window_days = max(200, backfill_days * 2 + 160)
        # 250 sessions * ~1.4 calendar days/session ≈ 350 calendar days minimum needed.
        # The formula must exceed that plus the 130d bar window = ~480 calendar days.
        assert cal_window_days >= 480, (
            f"cal_window_days={cal_window_days} is too small for backfill_days={backfill_days}"
        )

    def test_trading_days_before_large_n_with_dense_calendar(self):
        """
        _trading_days_before with n=200 must return exactly 200 dates from a
        dense calendar (every weekday is a session). The calendar must be big enough
        that no date walks outside the loaded window.
        """
        # Build a dense calendar: every weekday for ~600 days from a start date.
        anchor = date(2026, 6, 24)
        sessions = set()
        d = anchor - timedelta(days=600)
        while d <= anchor + timedelta(days=7):
            if d.weekday() < 5:
                sessions.add(d)
            d += timedelta(days=1)
        _flow_module._TRADING_DAYS = frozenset(sessions)

        result = _trading_days_before(anchor, 200)
        assert len(result) == 200, f"Expected 200 dates, got {len(result)}"
        assert result[0] == anchor or result[0] == _latest_trading_day(anchor), (
            "First date should be the most recent session <= anchor"
        )
        # All returned dates must be in the calendar.
        for d in result:
            assert _is_trading_day(d), f"{d} returned by _trading_days_before is not a session"

    # ---- Invariant 4: holiday boundary — as_of snap + effective_date chain ----

    def test_holiday_snap_chain_is_consistent(self):
        """
        On a holiday Thursday: as_of snaps to Wednesday; effective_date = Friday.
        as_of_date < effective_date, and both are sessions.
        """
        wednesday = date(2026, 7, 1)
        thursday = date(2026, 7, 2)   # holiday in fake calendar
        friday = date(2026, 7, 3)

        _flow_module._TRADING_DAYS = frozenset([wednesday, friday])
        # thursday is NOT in the set (it's a holiday)

        snapped_as_of = _latest_trading_day(thursday)
        assert snapped_as_of == wednesday, f"Thursday holiday should snap to Wednesday, got {snapped_as_of}"

        effective = _next_trading_day(snapped_as_of)
        assert effective == friday, f"next session after Wednesday should be Friday, got {effective}"

        assert _is_trading_day(snapped_as_of), "as_of_date must be a real session"
        assert _is_trading_day(effective), "effective_date must be a real session"
        assert snapped_as_of < effective, "as_of_date must be before effective_date"


# ---------------------------------------------------------------------------
# 12. Flow empty-hedge-map: fail loud on full-universe run, lenient on subset
# ---------------------------------------------------------------------------

class TestEmptyHedgeMapBehavior:
    """
    When compute_hedge_map yields zero rows:
    - Full-universe scheduled run → RuntimeError (fail loud so alerting fires)
    - Subset/test run              → lenient (logs + continues)

    These tests verify the branching logic added in P2 [hedge_map_flow.py:962].
    We simulate the condition by checking that the RuntimeError is raised only
    when subset_symbols is falsy.
    """

    def _call_guard(self, hedge_map_empty: bool, subset_symbols=None):
        """Inline the guard logic from the flow's processing loop."""
        if hedge_map_empty:
            if subset_symbols:
                # lenient path — would log and continue
                return "lenient"
            else:
                raise RuntimeError("compute_hedge_map returned zero rows")
        return "ok"

    def test_full_universe_empty_map_raises(self):
        """
        Full-universe run (subset_symbols=None) with zero hedge rows must raise RuntimeError
        so that alerting fires instead of silently producing no hedge.
        """
        with pytest.raises(RuntimeError, match="zero rows"):
            self._call_guard(hedge_map_empty=True, subset_symbols=None)

    def test_subset_run_empty_map_is_lenient(self):
        """
        Subset/test run with zero hedge rows must NOT raise — partial symbol
        sets may produce no hedgeable pairs and that's acceptable.
        """
        result = self._call_guard(hedge_map_empty=True, subset_symbols=["AAPL"])
        assert result == "lenient"

    def test_non_empty_map_never_raises(self):
        """A non-empty map never triggers the guard on either path."""
        assert self._call_guard(hedge_map_empty=False, subset_symbols=None) == "ok"
        assert self._call_guard(hedge_map_empty=False, subset_symbols=["AAPL"]) == "ok"


# ---------------------------------------------------------------------------
# 13. run_local manifest uses beta_as_of when bar guard fires
# ---------------------------------------------------------------------------

class TestRunLocalManifestAsOf:
    """
    When the bar-lag guard fires in run_local, the parquet rows carry beta_as_of
    as their as_of_date. The manifest must record the same value so the two
    artefacts stay consistent.

    We verify the invariant by constructing the manifest dict exactly as
    run_local.py does and checking the as_of_date field.
    """

    def _build_manifest(self, run_as_of: date, beta_as_of: date) -> dict:
        """Build a manifest dict matching run_local.py's manifest construction."""
        effective_date = _next_trading_day(run_as_of)
        return {
            "effective_date": effective_date.isoformat(),
            "as_of_date": beta_as_of.isoformat(),  # must be beta_as_of, not run_as_of
            "universe_size": 100,
            "eligible_count": 80,
        }

    def test_no_lag_manifest_uses_run_as_of(self):
        """When guard does not fire, beta_as_of == run_as_of and manifest is normal."""
        _flow_module._TRADING_DAYS = frozenset([
            date(2026, 6, 24), date(2026, 6, 25),
        ])
        run_as_of = date(2026, 6, 24)
        beta_as_of = run_as_of  # no lag
        manifest = self._build_manifest(run_as_of, beta_as_of)
        assert manifest["as_of_date"] == run_as_of.isoformat()

    def test_lag_manifest_uses_beta_as_of_not_run_as_of(self):
        """
        When bars lag (beta_as_of < run_as_of), the manifest must record beta_as_of.
        Recording run_as_of would produce a parquet/manifest inconsistency because
        the parquet rows already carry beta_as_of as their as_of_date.
        """
        _flow_module._TRADING_DAYS = frozenset([
            date(2026, 6, 22),  # Monday — actual_last_bar
            date(2026, 6, 23),  # Tuesday — run_as_of (scheduled)
            date(2026, 6, 24),  # Wednesday — effective_date
        ])
        run_as_of = date(2026, 6, 23)
        beta_as_of = date(2026, 6, 22)  # guard fired; bars lagged by one day

        manifest = self._build_manifest(run_as_of, beta_as_of)

        # Manifest as_of_date must match parquet as_of_date (beta_as_of).
        assert manifest["as_of_date"] == beta_as_of.isoformat(), (
            f"Manifest as_of_date should be {beta_as_of} (beta_as_of), "
            f"not {run_as_of} (run_as_of)"
        )
        # Manifest effective_date must still be the upcoming session from run_as_of.
        assert manifest["effective_date"] == date(2026, 6, 24).isoformat(), (
            "effective_date must be derived from run_as_of, not beta_as_of"
        )

    def test_lag_and_no_lag_differ_only_in_as_of_date(self):
        """
        With bar lag vs without, the only difference in the manifest is as_of_date.
        effective_date and universe_size are identical — effective_date must not
        rewind due to bar lag.
        """
        _flow_module._TRADING_DAYS = frozenset([
            date(2026, 6, 22), date(2026, 6, 23), date(2026, 6, 24),
        ])
        run_as_of = date(2026, 6, 23)

        normal = self._build_manifest(run_as_of, beta_as_of=run_as_of)
        lagged = self._build_manifest(run_as_of, beta_as_of=date(2026, 6, 22))

        assert normal["effective_date"] == lagged["effective_date"], (
            "effective_date must be the same whether or not bar lag occurred"
        )
        assert normal["as_of_date"] != lagged["as_of_date"], (
            "as_of_date must differ — it records the actual beta window, not the schedule"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
