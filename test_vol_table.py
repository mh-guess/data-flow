"""Tests for compute_volatility_row in vol_table_flow."""

import math
from datetime import datetime, timezone

import pandas as pd
import pytest

from vol_table_flow import compute_volatility_row, MIN_HISTORY_DAYS


@pytest.fixture
def computed_at():
    return datetime(2026, 5, 11, 22, 0, 0, tzinfo=timezone.utc)


def _make_ohlcv(prices: list[float], start_date: str = "2026-01-01") -> list[dict]:
    """Build minimal Tiingo-shaped OHLCV records from a list of adjClose values."""
    base = datetime.fromisoformat(start_date)
    records = []
    for i, price in enumerate(prices):
        d = base.replace(day=1) + pd.tseries.offsets.BDay(i)
        records.append({
            "date": d.strftime("%Y-%m-%dT00:00:00+00:00"),
            "close": price,
            "adjClose": price,
            "open": price,
            "high": price,
            "low": price,
            "volume": 1000000,
        })
    return records


class TestNormalCase:
    def test_known_values(self, computed_at):
        prices = [100.0, 102.0, 99.0, 101.0, 103.0, 98.0, 100.0, 105.0, 102.0, 104.0]
        ohlcv = _make_ohlcv(prices)

        log_returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))]
        expected_daily_vol = pd.Series(log_returns).std(ddof=1)
        expected_annualized = expected_daily_vol * math.sqrt(252)
        expected_per_minute = expected_daily_vol / math.sqrt(390)

        row = compute_volatility_row("TEST", ohlcv, computed_at)

        assert row["symbol"] == "TEST"
        assert row["no_data"] is False
        assert row["lookback_days"] == 10
        assert math.isclose(row["daily_vol"], expected_daily_vol, rel_tol=1e-10)
        assert math.isclose(row["daily_vol_annualized"], expected_annualized, rel_tol=1e-10)
        assert math.isclose(row["per_minute_vol"], expected_per_minute, rel_tol=1e-10)

    def test_dates_parsed(self, computed_at):
        ohlcv = _make_ohlcv([100.0, 101.0, 102.0])
        row = compute_volatility_row("TEST", ohlcv, computed_at)

        assert row["data_start_date"] is not None
        assert row["data_end_date"] is not None
        assert row["data_start_date"] < row["data_end_date"]

    def test_computed_at_passed_through(self, computed_at):
        ohlcv = _make_ohlcv([100.0, 101.0])
        row = compute_volatility_row("TEST", ohlcv, computed_at)
        assert row["computed_at"] == computed_at


class TestNoData:
    def test_none_input(self, computed_at):
        row = compute_volatility_row("GONE", None, computed_at)
        assert row["no_data"] is True
        assert row["daily_vol"] is None
        assert row["daily_vol_annualized"] is None
        assert row["per_minute_vol"] is None
        assert row["lookback_days"] == 0
        assert row["symbol"] == "GONE"

    def test_empty_list(self, computed_at):
        row = compute_volatility_row("GONE", [], computed_at)
        assert row["no_data"] is True
        assert row["lookback_days"] == 0

    def test_single_record(self, computed_at):
        ohlcv = _make_ohlcv([100.0])
        row = compute_volatility_row("ONE", ohlcv, computed_at)
        assert row["no_data"] is True


class TestShortHistory:
    def test_below_threshold(self, computed_at):
        prices = [100.0 + i * 0.5 for i in range(50)]
        ohlcv = _make_ohlcv(prices)
        row = compute_volatility_row("SHORT", ohlcv, computed_at)

        assert row["short_history"] is True
        assert row["no_data"] is False
        assert row["lookback_days"] == 50
        assert row["daily_vol"] is not None

    def test_at_threshold(self, computed_at):
        prices = [100.0 + i * 0.1 for i in range(MIN_HISTORY_DAYS)]
        ohlcv = _make_ohlcv(prices)
        row = compute_volatility_row("EXACT", ohlcv, computed_at)

        assert row["short_history"] is False
        assert row["lookback_days"] == MIN_HISTORY_DAYS


class TestNullAdjClose:
    def test_some_nulls_filtered(self, computed_at):
        ohlcv = _make_ohlcv([100.0, 101.0, 102.0, 103.0, 104.0])
        ohlcv[2]["adjClose"] = None

        row = compute_volatility_row("GAPS", ohlcv, computed_at)

        assert row["no_data"] is False
        assert row["lookback_days"] == 4

    def test_all_nulls(self, computed_at):
        ohlcv = _make_ohlcv([100.0, 101.0, 102.0])
        for r in ohlcv:
            r["adjClose"] = None

        row = compute_volatility_row("ALLNULL", ohlcv, computed_at)
        assert row["no_data"] is True
