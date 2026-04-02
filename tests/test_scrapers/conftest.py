"""Scraper-specific test fixtures."""

import pytest
from unittest.mock import AsyncMock, MagicMock

import httpx


def make_httpx_response(status_code: int = 200, json_data=None, text: str = ""):
    """Build a fake httpx.Response."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.text = text
    if json_data is not None:
        response.json.return_value = json_data
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"{status_code}", request=MagicMock(), response=response
        )
    return response


SAMPLE_CIK_TICKER_JSON = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
    "2": {"cik_str": 2488, "ticker": "AMD", "title": "Advanced Micro Devices Inc"},
}


SAMPLE_SUBMISSIONS_JSON = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "form": ["4", "4", "10-Q", "4"],
            "filingDate": ["2026-03-28", "2026-03-15", "2026-02-01", "2026-01-15"],
            "accessionNumber": ["0000320193-26-000001", "0000320193-26-000002", "0000320193-26-000003", "0000320193-26-000004"],
            "primaryDocument": ["xslF345X05/doc.xml", "filing.xml", "10q.htm", "old.xml"],
        }
    }
}


SAMPLE_FORM4_XML_BUY = """<?xml version="1.0"?>
<ownershipDocument>
    <issuer>
        <issuerCik>0000320193</issuerCik>
        <issuerName>Apple Inc.</issuerName>
        <issuerTradingSymbol>AAPL</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001234567</rptOwnerCik>
            <rptOwnerName>John Doe</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
            <officerTitle>SVP Engineering</officerTitle>
        </reportingOwnerRelationship>
    </reportingOwner>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <transactionDate><value>2026-03-25</value></transactionDate>
            <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
            <transactionAmounts>
                <transactionShares><value>1000</value></transactionShares>
                <transactionPricePerShare><value>185.50</value></transactionPricePerShare>
            </transactionAmounts>
        </nonDerivativeTransaction>
    </nonDerivativeTable>
</ownershipDocument>"""

SAMPLE_FORM4_XML_SELL = SAMPLE_FORM4_XML_BUY.replace(
    "<transactionCode>P</transactionCode>",
    "<transactionCode>S</transactionCode>",
)

SAMPLE_FORM4_XML_EXERCISE = SAMPLE_FORM4_XML_BUY.replace(
    "<transactionCode>P</transactionCode>",
    "<transactionCode>M</transactionCode>",
)

SAMPLE_FORM4_XML_UNKNOWN_CODE = SAMPLE_FORM4_XML_BUY.replace(
    "<transactionCode>P</transactionCode>",
    "<transactionCode>Z</transactionCode>",
)

SAMPLE_FORM4_XML_NO_DATE = SAMPLE_FORM4_XML_BUY.replace(
    "<transactionDate><value>2026-03-25</value></transactionDate>",
    "<transactionDate><value></value></transactionDate>",
)

SAMPLE_FORM4_XML_MISSING_ISSUER = """<?xml version="1.0"?>
<ownershipDocument>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001234567</rptOwnerCik>
            <rptOwnerName>John Doe</rptOwnerName>
        </reportingOwnerId>
    </reportingOwner>
</ownershipDocument>"""

SAMPLE_FORM4_XML_MALFORMED = """<?xml version="1.0"?>
<ownershipDocument>
    <issuer><issuerCik>broken"""
