"""Tests for the stock price scraper."""

import math
import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

import pandas as pd

from fathom.scrapers.stock_prices import (
    fetch_stock_prices,
    StockPriceItem,
    _safe_float,
    _safe_int,
)


class TestFetchStockPrices:
    @patch("fathom.scrapers.stock_prices.yf.download")
    def test_single_ticker(self, mock_download):
        """Should parse single-ticker yfinance response."""
        dates = pd.to_datetime(["2026-03-28", "2026-03-31"])
        mock_download.return_value = pd.DataFrame(
            {
                "Open": [150.0, 152.0],
                "High": [155.0, 157.0],
                "Low": [149.0, 151.0],
                "Close": [154.0, 156.0],
                "Adj Close": [154.0, 156.0],
                "Volume": [1000000, 1200000],
            },
            index=dates,
        )

        items = fetch_stock_prices(["AAPL"], days=5)

        assert len(items) == 2
        assert all(isinstance(item, StockPriceItem) for item in items)
        assert items[0].ticker == "AAPL"
        assert items[0].close == 154.0
        assert items[1].volume == 1200000

    @patch("fathom.scrapers.stock_prices.yf.download")
    def test_multiple_tickers(self, mock_download):
        """Should parse multi-ticker yfinance response."""
        dates = pd.to_datetime(["2026-03-28"])
        # yfinance with group_by="ticker" returns (ticker, column) MultiIndex
        columns = pd.MultiIndex.from_tuples(
            [("AAPL", "Open"), ("AAPL", "High"), ("AAPL", "Low"),
             ("AAPL", "Close"), ("AAPL", "Adj Close"), ("AAPL", "Volume"),
             ("MSFT", "Open"), ("MSFT", "High"), ("MSFT", "Low"),
             ("MSFT", "Close"), ("MSFT", "Adj Close"), ("MSFT", "Volume")],
        )
        data = pd.DataFrame(
            [[150.0, 155.0, 149.0, 154.0, 154.0, 1000000,
              400.0, 410.0, 398.0, 405.0, 405.0, 500000]],
            index=dates,
            columns=columns,
        )
        mock_download.return_value = data

        items = fetch_stock_prices(["AAPL", "MSFT"], days=5)

        assert len(items) == 2
        tickers = {item.ticker for item in items}
        assert tickers == {"AAPL", "MSFT"}

    def test_empty_tickers(self):
        """Empty ticker list should return empty list without calling yfinance."""
        items = fetch_stock_prices([], days=5)
        assert items == []

    @patch("fathom.scrapers.stock_prices.yf.download")
    def test_no_data_returned(self, mock_download):
        """Empty DataFrame should return empty list."""
        mock_download.return_value = pd.DataFrame()

        items = fetch_stock_prices(["FAKE"], days=5)
        assert items == []

    @patch("fathom.scrapers.stock_prices.yf.download")
    def test_exception_returns_empty(self, mock_download):
        """yfinance exceptions should be caught and return empty."""
        mock_download.side_effect = Exception("Network error")

        items = fetch_stock_prices(["AAPL"], days=5)
        assert items == []

    @patch("fathom.scrapers.stock_prices.yf.download")
    def test_missing_ticker_in_multi(self, mock_download):
        """Missing ticker in multi-download should be skipped."""
        dates = pd.to_datetime(["2026-03-28"])
        columns = pd.MultiIndex.from_tuples(
            [("Open", "AAPL"), ("High", "AAPL"), ("Low", "AAPL"),
             ("Close", "AAPL"), ("Adj Close", "AAPL"), ("Volume", "AAPL")],
        )
        data = pd.DataFrame(
            [[150.0, 155.0, 149.0, 154.0, 154.0, 1000000]],
            index=dates,
            columns=columns,
        )
        mock_download.return_value = data

        # Request MSFT too, but it's not in the response
        items = fetch_stock_prices(["AAPL", "MSFT"], days=5)

        # Should get AAPL but skip MSFT
        assert all(item.ticker == "AAPL" for item in items)


class TestSafeFloat:
    def test_normal_value(self):
        assert _safe_float(42.5) == 42.5

    def test_nan_returns_none(self):
        assert _safe_float(float("nan")) is None

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_string_returns_none(self):
        assert _safe_float("not a number") is None

    def test_rounds_to_4_decimals(self):
        assert _safe_float(1.23456789) == 1.2346


class TestSafeInt:
    def test_normal_value(self):
        assert _safe_int(42.0) == 42

    def test_nan_returns_none(self):
        assert _safe_int(float("nan")) is None

    def test_none_returns_none(self):
        assert _safe_int(None) is None
