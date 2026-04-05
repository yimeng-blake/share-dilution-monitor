"""Data access layer for the SEC_FILINGS data lake.

Provides CRUD operations for SEC_FILINGS.PUBLIC.FILINGS — full-text
filing documents stored for cross-project reuse.

Uses the same Snowflake connection as snowflake_dilution (shared session).
"""

import logging
from typing import Optional

from services.snowflake_dilution import get_session

logger = logging.getLogger(__name__)


def _execute(sql: str, params=None) -> list[dict]:
    from snowflake.connector import DictCursor
    conn = get_session()
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()


def _execute_no_fetch(sql: str, params=None) -> int:
    conn = get_session()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.rowcount
    finally:
        cur.close()


def filing_exists(cik: str, accession_number: str) -> bool:
    """Check if a filing already exists in the data lake."""
    rows = _execute(
        "SELECT 1 FROM SEC_FILINGS.PUBLIC.FILINGS "
        "WHERE CIK = %s AND ACCESSION_NUMBER = %s",
        (cik, accession_number),
    )
    return len(rows) > 0


def insert_filing(
    cik: str,
    ticker: str,
    entity_name: str,
    accession_number: str,
    form_type: str,
    filed_date: Optional[str],
    report_date: Optional[str],
    primary_document: str,
    filing_url: str,
    document_text: str,
) -> bool:
    """Insert a filing into the data lake. Returns True if inserted, False if duplicate."""
    if filing_exists(cik, accession_number):
        return False

    doc_length = len(document_text) if document_text else 0
    try:
        _execute_no_fetch(
            "INSERT INTO SEC_FILINGS.PUBLIC.FILINGS "
            "(CIK, TICKER, ENTITY_NAME, ACCESSION_NUMBER, FORM_TYPE, "
            "FILED_DATE, REPORT_DATE, PRIMARY_DOCUMENT, FILING_URL, "
            "DOCUMENT_TEXT, DOCUMENT_LENGTH) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                cik, ticker.upper(), entity_name, accession_number, form_type,
                filed_date, report_date, primary_document, filing_url,
                document_text, doc_length,
            ),
        )
        return True
    except Exception as e:
        logger.error(f"Failed to insert filing {accession_number}: {e}")
        return False


def get_filings_for_ticker(
    ticker: str,
    form_types: Optional[tuple[str, ...]] = None,
) -> list[dict]:
    """Get all filings for a ticker from the data lake.

    Returns metadata only (no document text) for listing purposes.
    """
    sql = (
        "SELECT CIK, TICKER, ENTITY_NAME, ACCESSION_NUMBER, FORM_TYPE, "
        "FILED_DATE, REPORT_DATE, PRIMARY_DOCUMENT, FILING_URL, "
        "DOCUMENT_LENGTH, INGESTED_AT "
        "FROM SEC_FILINGS.PUBLIC.FILINGS "
        "WHERE TICKER = %s "
    )
    params = [ticker.upper()]

    if form_types:
        placeholders = ", ".join(["%s"] * len(form_types))
        sql += f"AND FORM_TYPE IN ({placeholders}) "
        params.extend(form_types)

    sql += "ORDER BY REPORT_DATE"
    return _execute(sql, tuple(params))


def get_filing_text(cik: str, accession_number: str) -> Optional[str]:
    """Retrieve the full document text for a specific filing."""
    rows = _execute(
        "SELECT DOCUMENT_TEXT FROM SEC_FILINGS.PUBLIC.FILINGS "
        "WHERE CIK = %s AND ACCESSION_NUMBER = %s",
        (cik, accession_number),
    )
    if rows:
        return rows[0].get("DOCUMENT_TEXT")
    return None


def get_filing_count(ticker: str) -> int:
    """Get count of filings stored for a ticker."""
    rows = _execute(
        "SELECT COUNT(*) AS CNT FROM SEC_FILINGS.PUBLIC.FILINGS WHERE TICKER = %s",
        (ticker.upper(),),
    )
    return rows[0]["CNT"] if rows else 0
