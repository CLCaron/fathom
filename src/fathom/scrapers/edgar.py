"""SEC EDGAR Form 4 scraper for insider trades.

Uses the EDGAR submissions API (data.sec.gov) to fetch recent Form 4 filings,
then parses the XML content to extract trade details.
"""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from fathom.config import settings
from fathom.scrapers.base import BaseScraper, ScrapedItem

logger = logging.getLogger(__name__)

CIK_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

# Major companies to track for insider trades (CIK numbers).
# This list covers S&P 500 heavy-hitters across key sectors.
# We expand this over time or switch to a feed-based approach.
TRACKED_CIKS = {
    # Technology
    "320193": "AAPL", "789019": "MSFT", "1652044": "GOOGL",
    "1018724": "AMZN", "1326801": "META", "1045810": "NVDA",
    "2488": "AMD", "50863": "INTC", "1108524": "CRM",
    "1341439": "ORCL", "796343": "ADBE", "1649338": "AVGO",
    "858877": "CSCO", "804328": "QCOM", "97476": "TXN",
    "51143": "IBM", "6951": "AMAT", "723125": "MU",
    "1373715": "NOW", "1327567": "PANW", "883241": "SNPS",
    "813672": "CDNS",
    # Semiconductors
    "1058057": "MRVL", "319201": "KLAC", "707549": "LRCX",
    # Energy
    "34088": "XOM", "93410": "CVX", "797468": "OXY",
    "1163165": "COP", "87347": "SLB", "821189": "EOG",
    "19808": "MPC", "764065": "PSX", "1035002": "VLO",
    "45012": "HAL",
    # Defense
    "936468": "LMT", "101829": "RTX", "40159": "NOC",
    "40533": "GD", "12927": "BA", "202058": "LHX",
    "1501585": "HII",
    # Finance
    "19617": "JPM", "70858": "BAC", "72971": "WFC",
    "886982": "GS", "895421": "MS", "831001": "C",
    "1364742": "BLK", "316709": "SCHW",
    "4962": "AXP", "36104": "USB", "713676": "PNC",
    "92230": "TFC", "1156375": "CME",
    # Healthcare
    "200406": "JNJ", "731766": "UNH", "78003": "PFE",
    "1551152": "ABBV", "310158": "MRK", "59478": "LLY",
    "97745": "TMO", "1800": "ABT",
    "14272": "BMY", "318154": "AMGN", "882095": "GILD",
    "1613103": "MDT", "1035267": "ISRG", "310764": "SYK",
    # Consumer
    "104169": "WMT", "80424": "PG", "21344": "KO",
    "77476": "PEP", "909832": "COST", "354950": "HD",
    "63908": "MCD", "320187": "NKE",
    "829224": "SBUX", "27419": "TGT", "60667": "LOW",
    "109198": "TJX", "1075531": "BKNG",
    # Telecom
    "732717": "T", "732712": "VZ", "1283699": "TMUS",
    # Industrial
    "18230": "CAT", "315189": "DE", "1090727": "UPS",
    "773840": "HON", "40554": "GE", "66740": "MMM",
    # Real Estate
    "1053507": "AMT", "1045609": "PLD", "1051470": "CCI",
    "1063761": "SPG", "726728": "O",
    # Utilities
    "753308": "NEE", "1326160": "DUK", "92122": "SO",
    "715957": "D", "4904": "AEP",
}


@dataclass
class InsiderTradeItem(ScrapedItem):
    cik: str
    filer_name: str
    filer_title: str | None
    company_name: str
    ticker: str | None
    trade_type: str  # BUY, SELL, EXERCISE
    shares: int | None
    price_per_share: float | None
    total_value: float | None
    trade_date: date
    filing_date: datetime
    filing_url: str | None


class EdgarScraper(BaseScraper):
    """Scrapes SEC EDGAR for Form 4 (insider trade) filings."""

    def __init__(self):
        super().__init__(rate_limit_delay=0.15, max_retries=3)
        self._cik_ticker_map: dict[str, str] = {}
        self._headers = {
            "User-Agent": settings.sec_edgar_user_agent,
            "Accept": "application/json",
        }

    async def _load_cik_ticker_map(self):
        """Load the CIK-to-ticker mapping from SEC."""
        if self._cik_ticker_map:
            return

        logger.info("Loading CIK-to-ticker mapping from SEC...")
        response = await self._fetch(CIK_TICKER_MAP_URL, headers=self._headers)
        data = response.json()

        for entry in data.values():
            cik = str(entry["cik_str"]).zfill(10)
            ticker = entry.get("ticker", "")
            if ticker:
                self._cik_ticker_map[cik] = ticker.upper()

        logger.info(f"Loaded {len(self._cik_ticker_map)} CIK-to-ticker mappings")

    def _cik_to_ticker(self, cik: str) -> str | None:
        return self._cik_ticker_map.get(cik.zfill(10))

    async def _get_recent_form4_filings(self, cik: str, days_back: int = 30) -> list[dict]:
        """Get recent Form 4 filings for a company via the submissions API."""
        padded_cik = cik.zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"

        try:
            response = await self._fetch(url, headers=self._headers)
            data = response.json()
        except Exception as e:
            logger.warning(f"Failed to fetch submissions for CIK {cik}: {e}")
            return []

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        filings = []

        for i, form_type in enumerate(forms):
            if form_type != "4":
                continue
            if i >= len(filing_dates) or filing_dates[i] < cutoff:
                continue

            accession = accession_numbers[i].replace("-", "")
            primary = primary_docs[i]

            # Strip the XSLT prefix if present (e.g., "xslF345X05/filename.xml")
            raw_filename = primary.split("/")[-1] if "/" in primary else primary

            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{raw_filename}"
            filings.append({
                "xml_url": xml_url,
                "filing_date": filing_dates[i],
                "accession": accession_numbers[i],
                "cik": cik,
            })

        return filings

    async def _fetch_and_parse_form4(self, filing: dict) -> list[InsiderTradeItem]:
        """Fetch and parse a Form 4 XML document."""
        xml_url = filing["xml_url"]
        items = []

        try:
            response = await self._fetch(xml_url, headers=self._headers)
            items = self._parse_form4_xml(response.text, xml_url, filing["filing_date"])
        except Exception as e:
            logger.warning(f"Failed to parse Form 4 at {xml_url}: {e}")

        return items

    def _parse_form4_xml(self, xml_text: str, filing_url: str, filing_date_str: str) -> list[InsiderTradeItem]:
        """Parse a Form 4 XML document into InsiderTradeItem objects."""
        items = []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"XML parse error for {filing_url}: {e}")
            return items

        issuer = root.find(".//issuer")
        owner = root.find(".//reportingOwner")

        if issuer is None or owner is None:
            return items

        issuer_cik = self._get_text(issuer, "issuerCik", "")
        company_name = self._get_text(issuer, "issuerName", "Unknown")
        issuer_ticker = self._get_text(issuer, "issuerTradingSymbol", "")

        owner_name = self._get_text(owner, ".//rptOwnerName", "Unknown")
        owner_cik = self._get_text(owner, ".//rptOwnerCik", "")

        relationship = owner.find(".//reportingOwnerRelationship")
        title = ""
        if relationship is not None:
            title = self._get_text(relationship, "officerTitle", "")
            if not title:
                if self._get_text(relationship, "isDirector", "0") == "1":
                    title = "Director"
                elif self._get_text(relationship, "isTenPercentOwner", "0") == "1":
                    title = "10% Owner"

        ticker = issuer_ticker.upper().strip() if issuer_ticker else self._cik_to_ticker(issuer_cik)

        filing_date = datetime.fromisoformat(filing_date_str) if filing_date_str else datetime.utcnow()

        for txn in root.findall(".//nonDerivativeTransaction"):
            item = self._parse_transaction(
                txn, owner_cik, owner_name, title, company_name,
                ticker, filing_url, filing_date
            )
            if item:
                items.append(item)

        for txn in root.findall(".//derivativeTransaction"):
            item = self._parse_transaction(
                txn, owner_cik, owner_name, title, company_name,
                ticker, filing_url, filing_date, is_derivative=True
            )
            if item:
                items.append(item)

        return items

    def _parse_transaction(
        self, txn, cik: str, filer_name: str, title: str,
        company_name: str, ticker: str | None, filing_url: str,
        filing_date: datetime, is_derivative: bool = False
    ) -> InsiderTradeItem | None:
        """Parse a single transaction element from Form 4 XML."""
        try:
            date_str = self._get_text(txn, ".//transactionDate/value", "")
            if not date_str:
                return None
            trade_date = date.fromisoformat(date_str)

            code = self._get_text(txn, ".//transactionCoding/transactionCode", "")
            if code == "P":
                trade_type = "BUY"
            elif code == "S":
                trade_type = "SELL"
            elif code in ("M", "C", "A"):
                trade_type = "EXERCISE"
            else:
                return None

            shares_str = self._get_text(txn, ".//transactionAmounts/transactionShares/value", "0")
            price_str = self._get_text(txn, ".//transactionAmounts/transactionPricePerShare/value", "0")

            shares = int(float(shares_str)) if shares_str else None
            price = float(price_str) if price_str and price_str != "0" else None

            total_value = None
            if shares and price:
                total_value = round(shares * price, 2)

            return InsiderTradeItem(
                source="edgar",
                cik=cik,
                filer_name=filer_name,
                filer_title=title or None,
                company_name=company_name,
                ticker=ticker,
                trade_type=trade_type,
                shares=shares,
                price_per_share=price,
                total_value=total_value,
                trade_date=trade_date,
                filing_date=filing_date,
                filing_url=filing_url,
            )
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse transaction: {e}")
            return None

    def _get_text(self, element, path: str, default: str = "") -> str:
        el = element.find(path)
        return el.text.strip() if el is not None and el.text else default

    async def scrape(self) -> list[InsiderTradeItem]:
        """Scrape recent Form 4 filings from SEC EDGAR."""
        await self._load_cik_ticker_map()

        logger.info(f"Fetching recent Form 4 filings for {len(TRACKED_CIKS)} companies...")
        all_items: list[InsiderTradeItem] = []
        total_filings = 0

        for cik, ticker in TRACKED_CIKS.items():
            filings = await self._get_recent_form4_filings(cik, days_back=90)
            total_filings += len(filings)

            for filing in filings:
                items = await self._fetch_and_parse_form4(filing)
                all_items.extend(items)

        logger.info(f"Scraped {len(all_items)} insider trades from {total_filings} filings across {len(TRACKED_CIKS)} companies")
        return all_items
