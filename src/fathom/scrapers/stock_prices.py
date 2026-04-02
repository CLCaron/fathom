"""Stock price scraper using yfinance."""

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import yfinance as yf

from fathom.scrapers.base import ScrapedItem

logger = logging.getLogger(__name__)


@dataclass
class StockPriceItem(ScrapedItem):
    ticker: str
    date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    adj_close: float | None
    volume: int | None


def fetch_stock_prices(tickers: list[str], days: int = 5) -> list[StockPriceItem]:
    """Fetch recent stock prices for a list of tickers.

    This is synchronous because yfinance uses requests internally.
    Run in an executor from async code.
    """
    items = []
    end = date.today()
    start = end - timedelta(days=days)

    if not tickers:
        return items

    # yfinance can batch download multiple tickers
    try:
        data = yf.download(
            tickers=" ".join(tickers),
            start=start.isoformat(),
            end=end.isoformat(),
            group_by="ticker" if len(tickers) > 1 else "column",
            progress=False,
            auto_adjust=False,
        )

        if data.empty:
            logger.warning("No stock price data returned")
            return items

        if len(tickers) == 1:
            ticker = tickers[0]
            for idx, row in data.iterrows():
                trade_date = idx.date() if hasattr(idx, "date") else idx
                items.append(StockPriceItem(
                    source="yfinance",
                    ticker=ticker,
                    date=trade_date,
                    open=_safe_float(row.get("Open")),
                    high=_safe_float(row.get("High")),
                    low=_safe_float(row.get("Low")),
                    close=_safe_float(row.get("Close")),
                    adj_close=_safe_float(row.get("Adj Close")),
                    volume=_safe_int(row.get("Volume")),
                ))
        else:
            for ticker in tickers:
                try:
                    ticker_data = data[ticker]
                    for idx, row in ticker_data.iterrows():
                        trade_date = idx.date() if hasattr(idx, "date") else idx
                        items.append(StockPriceItem(
                            source="yfinance",
                            ticker=ticker,
                            date=trade_date,
                            open=_safe_float(row.get("Open")),
                            high=_safe_float(row.get("High")),
                            low=_safe_float(row.get("Low")),
                            close=_safe_float(row.get("Close")),
                            adj_close=_safe_float(row.get("Adj Close")),
                            volume=_safe_int(row.get("Volume")),
                        ))
                except (KeyError, AttributeError):
                    logger.warning(f"No data for ticker {ticker}")
                    continue

    except Exception as e:
        logger.error(f"Failed to fetch stock prices: {e}")

    logger.info(f"Fetched {len(items)} price records for {len(tickers)} tickers")
    return items


def _safe_float(val) -> float | None:
    try:
        import math
        f = float(val)
        return None if math.isnan(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        import math
        f = float(val)
        return None if math.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None
