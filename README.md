# Fathom

A financial research tool that connects insider trades, congressional trades, and major events to historical patterns -- then shows you what happened next.

## What It Does

**Insider trade tracking:**
- Scrapes SEC EDGAR for insider trade filings (Form 4) across major companies
- Tracks stock prices for context and performance measurement
- Filterable dashboard with sector, type, and time range filters

**Congressional trade tracking:**
- Scrapes Capitol Trades for House and Senate STOCK Act disclosures
- Committee membership data from congress.gov (with sector coverage mapping)
- Legislation tracking with keyword-based sector tagging
- Dedicated congressional trades dashboard with chamber, party, and sector filters

**Automated scheduling:**
- SEC EDGAR scraper runs every 15 minutes
- Capitol Trades scraper runs daily
- Committee and legislation scrapers run on configurable intervals
- All scrapers can be triggered manually from the admin page

**Planned:**
- Correlation engine linking congressional trades to committee oversight and legislation timing
- Event analysis system: log a news event and see how similar events played out historically
- Market context engine for meaningful historical comparisons
- Trade clustering detection (multiple actors, same sector, same timeframe)
- Historical pattern matching with multi-timeframe analysis (7d, 30d, 90d)

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

The default settings work out of the box. Update `SEC_EDGAR_USER_AGENT` with your name and email (SEC requires this for API access). Optionally add a `CONGRESS_API_KEY` from congress.gov for committee and legislation data.

### Run

```bash
python -m uvicorn fathom.main:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000 in your browser.

## Usage

### Insider Trades

The main page shows recent insider trades with filters for time range, trade type, and sector.

### Congressional Trades

http://127.0.0.1:8000/congressional shows congressional stock trades with filters for chamber, party, trade type, and sector. Committee memberships and legislation are available as sub-pages.

### Admin

http://127.0.0.1:8000/admin shows database stats and lets you manually trigger any scraper. Buttons show loading state and formatted results.

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
| Capitol Trades | Congressional stock trades (STOCK Act) | Free |
| congress.gov | Committee memberships, legislation, votes | Free (API key) |
| yfinance | Daily stock prices, VIX, sector ETFs | Free |
| FRED | Economic indicators, interest rates (planned) | Free |
| NewsAPI / RSS | News events (planned) | Free tier |

## Project Structure

```
financial-signals/
├── src/fathom/
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
