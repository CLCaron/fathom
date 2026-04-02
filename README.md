# Fathom

A financial research tool that connects insider trades, congressional trades, and major events to historical patterns -- then shows you what happened next.

## What It Does

**Trade tracking:**
- Scrapes SEC EDGAR for insider trade filings (Form 4) across major companies
- Tracks stock prices for context and performance measurement
- Displays trades in a filterable dashboard with sector, type, and time range filters
- Runs on a schedule to pull new data automatically

**Planned:**
- Congressional trade tracking with committee oversight and legislation timing correlations
- Event analysis system: log a news event and see how similar events played out historically
- Market context engine that makes historical comparisons meaningful (matching on market conditions, not just event type)
- Trade clustering detection (multiple actors, same sector, same timeframe)
- ETF fund flow anomaly detection
- Historical pattern matching with multi-timeframe analysis (7d, 30d, 90d)
- Signal accuracy tracking and performance scorecards
- Inline education throughout (explains what you're looking at with links to learn more)

## Setup

### Requirements

- Python 3.12+
- pip

### Install

```bash
git clone <repo-url>
cd financial-signals
pip install -e ".[dev]"
```

### Configure

Copy the example environment file and update it:

```bash
cp .env.example .env
```

The default settings work out of the box. Update `SEC_EDGAR_USER_AGENT` with your name and email (SEC requires this for API access).

### Run

```bash
python -m uvicorn fathom.main:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000 in your browser.

## Usage

### Dashboard

The main page shows recent insider trades with filters for:

- **Time range** - today, 7 days, 14 days, 30 days, 90 days
- **Trade type** - buys, sells, or all
- **Sector** - technology, energy, defense, finance, healthcare, consumer, industrial, etc.

### Admin

http://127.0.0.1:8000/admin shows database stats and lets you manually trigger scrapers.

### Scheduler

The app runs an automated scheduler that pulls new SEC filings every 15 minutes while the server is running.

## Tech Stack

- **Python 3.12** with FastAPI
- **SQLite** via SQLAlchemy 2.0 (async)
- **HTMX** for dynamic page updates
- **APScheduler** for automated data collection
- **yfinance** for stock price data
- **httpx** for async HTTP requests

## Data Sources

| Source | What | Cost |
|--------|------|------|
| SEC EDGAR | Insider trade filings (Form 4) | Free |
| yfinance | Daily stock prices, VIX, sector ETFs | Free |
| congress.gov | Congressional trades, committees, votes (planned) | Free |
| FRED | Economic indicators, interest rates (planned) | Free |
| NewsAPI / RSS | News events (planned) | Free tier |

## Project Structure

```
financial-signals/
├── src/signals/
│   ├── models/        # Database models
│   ├── scrapers/      # Data collection
│   ├── engine/        # Pipeline, correlation, scoring
│   ├── api/           # FastAPI routes
│   ├── templates/     # HTML templates
│   └── static/        # CSS and JS
├── scripts/           # One-time data scripts
└── tests/             # Test suite
```

## License

Private project. Not licensed for redistribution.
