"""Snowflake data access layer for the Dilution Monitor.

Provides connection management and CRUD operations for:
- DILUTED_SHARES, BASIC_SHARES, BUYBACK_ACTIVITY, BUYBACK_PROGRAMS, INGESTION_LOG

Watchlist operations use the centralised WATCHLIST_HUB.PUBLIC.COMPANIES table.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

from config import settings

logger = logging.getLogger(__name__)

_conn = None


def _get_streamlit_secrets() -> Optional[dict]:
    """Try to read Snowflake config from Streamlit secrets (for Streamlit Cloud)."""
    try:
        import streamlit as st

        sf_secrets = st.secrets.get("snowflake")
        if sf_secrets and sf_secrets.get("password"):
            return {
                "account": sf_secrets["account"],
                "user": sf_secrets["user"],
                "password": sf_secrets["password"],
                "warehouse": sf_secrets.get("warehouse", "COMPUTE_WH"),
                "database": sf_secrets.get("database", "DILUTION_MONITOR"),
                "schema": sf_secrets.get("schema", "PUBLIC"),
                "role": sf_secrets.get("role", ""),
            }
    except Exception:
        pass
    return None


def get_session():
    """Get or create a Snowflake connection.

    Priority:
      1. st.session_state (persists across Streamlit reruns, avoids repeated OAuth)
      2. module-global _conn (fallback for non-Streamlit callers)
      3. Create new: st.secrets > env vars > connections.toml
    """
    global _conn

    # --- Try st.session_state first (survives Streamlit reruns) ---
    try:
        import streamlit as st
        cached = st.session_state.get("_sf_conn")
        if cached is not None and not cached.is_closed():
            return cached
    except Exception:
        pass

    # --- Fall back to module-global ---
    if _conn is not None and not _conn.is_closed():
        return _conn

    # --- Create a new connection ---
    st_config = _get_streamlit_secrets()
    if st_config:
        role = st_config.pop("role", "")
        if role:
            st_config["role"] = role
        _conn = snowflake.connector.connect(**st_config)
    elif settings.SNOWFLAKE_PASSWORD:
        _conn = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
        )
    else:
        import toml
        from pathlib import Path

        toml_path = Path.home() / ".snowflake" / "connections.toml"
        conn_name = None
        if toml_path.exists():
            toml_data = toml.load(toml_path)
            conn_name = toml_data.get("default_connection_name")

        kwargs = {
            "database": settings.SNOWFLAKE_DATABASE,
            "schema": settings.SNOWFLAKE_SCHEMA,
            "warehouse": settings.SNOWFLAKE_WAREHOUSE,
        }
        if conn_name:
            kwargs["connection_name"] = conn_name
        if settings.SNOWFLAKE_ROLE:
            kwargs["role"] = settings.SNOWFLAKE_ROLE
        _conn = snowflake.connector.connect(**kwargs)

    # --- Stash in st.session_state so it persists across reruns ---
    try:
        import streamlit as st
        st.session_state["_sf_conn"] = _conn
    except Exception:
        pass

    return _conn


def close_session():
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
    try:
        import streamlit as st
        st.session_state.pop("_sf_conn", None)
    except Exception:
        pass


def _execute(sql: str, params=None) -> list[dict]:
    """Execute SQL and return results as a list of dicts."""
    conn = get_session()
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()


def _execute_no_fetch(sql: str, params=None) -> int:
    """Execute SQL without fetching results (for INSERT/UPDATE/MERGE).

    Returns the number of rows affected.
    """
    conn = get_session()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.rowcount
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Centralised watchlist (WATCHLIST_HUB)
# ---------------------------------------------------------------------------


def get_watchlist() -> list[dict]:
    """Read the global watchlist from WATCHLIST_HUB."""
    try:
        return _execute(
            "SELECT TICKER, COMPANY_NAME, CIK, EXCHANGE "
            "FROM WATCHLIST_HUB.PUBLIC.COMPANIES "
            "WHERE ACTIVE = TRUE ORDER BY TICKER"
        )
    except Exception as e:
        logger.warning(f"Could not read watchlist: {e}")
        return []


def add_to_watchlist(
    ticker: str, company_name: str, cik: str, exchange: str = ""
) -> bool:
    """Add a company to the centralised watchlist.

    Returns True if inserted or reactivated, False if already active.
    """
    rows = _execute_no_fetch(
        "MERGE INTO WATCHLIST_HUB.PUBLIC.COMPANIES tgt "
        "USING (SELECT %s AS TICKER) src ON tgt.TICKER = src.TICKER "
        "WHEN MATCHED AND tgt.ACTIVE = FALSE THEN UPDATE SET "
        "  ACTIVE = TRUE, COMPANY_NAME = %s, CIK = %s, EXCHANGE = %s, "
        "  ADDED_AT = CURRENT_TIMESTAMP(), ADDED_BY = 'dilution_monitor' "
        "WHEN NOT MATCHED THEN INSERT "
        "  (TICKER, COMPANY_NAME, CIK, EXCHANGE, ADDED_BY) "
        "  VALUES (%s, %s, %s, %s, 'dilution_monitor')",
        (ticker.upper(), company_name, cik, exchange,
         ticker.upper(), company_name, cik, exchange),
    )
    return rows > 0


def remove_from_watchlist(ticker: str) -> bool:
    """Soft-delete a company from the centralised watchlist.

    Returns True if a row was deactivated.
    """
    rows = _execute_no_fetch(
        "UPDATE WATCHLIST_HUB.PUBLIC.COMPANIES SET ACTIVE = FALSE "
        "WHERE TICKER = %s AND ACTIVE = TRUE",
        (ticker.upper(),),
    )
    return rows > 0


# ---------------------------------------------------------------------------
# Ingestion log
# ---------------------------------------------------------------------------


def get_ingestion_status(ticker: str) -> Optional[dict]:
    """Get the ingestion log entry for a ticker."""
    rows = _execute(
        "SELECT * FROM INGESTION_LOG WHERE TICKER = %s", (ticker.upper(),)
    )
    return rows[0] if rows else None


def upsert_ingestion_log(
    ticker: str,
    cik: str,
    entity_name: str,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
    ipo_date: Optional[str] = None,
):
    """Insert or update ingestion log for a ticker."""
    now = datetime.now(timezone.utc)
    _execute_no_fetch(
        "MERGE INTO INGESTION_LOG t "
        "USING (SELECT %s AS TICKER) s ON t.TICKER = s.TICKER "
        "WHEN MATCHED THEN UPDATE SET "
        "  CIK = %s, ENTITY_NAME = %s, LAST_INGESTED_AT = %s, "
        "  STATUS = %s, ERROR_MESSAGE = %s, IPO_DATE = %s "
        "WHEN NOT MATCHED THEN INSERT "
        "  (TICKER, CIK, ENTITY_NAME, LAST_INGESTED_AT, STATUS, ERROR_MESSAGE, IPO_DATE) "
        "  VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            ticker.upper(),
            cik, entity_name, now, status, error_message, ipo_date,
            ticker.upper(), cik, entity_name, now, status, error_message, ipo_date,
        ),
    )


def get_all_ingested_tickers() -> list[dict]:
    """Get all tickers we've ingested, with status."""
    return _execute(
        "SELECT TICKER, CIK, ENTITY_NAME, LAST_INGESTED_AT, STATUS "
        "FROM INGESTION_LOG ORDER BY TICKER"
    )


def get_ipo_date(ticker: str) -> Optional[str]:
    """Get the stored IPO date for a ticker, or None if not set."""
    rows = _execute(
        "SELECT IPO_DATE FROM INGESTION_LOG WHERE TICKER = %s",
        (ticker.upper(),),
    )
    if rows and rows[0].get("IPO_DATE"):
        return str(rows[0]["IPO_DATE"])
    return None


def get_verified_splits(ticker: str) -> list[dict]:
    """Get verified stock splits from Cortex AI-extracted filing facts.

    Returns deduplicated splits after IPO date, sorted by date.
    Each dict has keys: date (str), ratio (int).
    """
    ipo_date = get_ipo_date(ticker)

    # Query splits from FILING_FACTS, filtering out pre-IPO splits
    rows = _execute(
        "SELECT STOCK_SPLIT_RATIO, STOCK_SPLIT_DATE, PERIOD_END "
        "FROM FILING_FACTS "
        "WHERE TICKER = %s AND STOCK_SPLIT_RATIO IS NOT NULL "
        "  AND STOCK_SPLIT_DATE IS NOT NULL "
        "ORDER BY STOCK_SPLIT_DATE",
        (ticker.upper(),),
    )

    if not rows:
        return []

    seen = set()  # (ratio, year) to deduplicate same split across filings
    splits = []
    for row in rows:
        split_date = str(row["STOCK_SPLIT_DATE"])
        ratio_raw = str(row["STOCK_SPLIT_RATIO"])

        # Filter out pre-IPO splits
        if ipo_date and split_date < ipo_date:
            continue

        # Parse ratio: "5:1" -> 5, "2" -> 2, "5-for-1" -> 5
        ratio_str = ratio_raw.replace("-for-", ":").replace(" ", "")
        if ":" in ratio_str:
            try:
                num = int(ratio_str.split(":")[0])
            except ValueError:
                continue
        else:
            try:
                num = int(float(ratio_str))
            except ValueError:
                continue

        if num <= 1:
            continue

        # Deduplicate: same ratio + same year = same split reported in
        # multiple filings.  Different years = distinct split events
        # (e.g. AAPL 2:1 in 2000 AND 2:1 in 2005).
        dedup_key = (num, split_date[:4])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        splits.append({
            "date": split_date,
            "ratio": num,
        })

    return splits


def get_filing_facts_count(ticker: str) -> int:
    """Get count of filing facts extracted for a ticker."""
    rows = _execute(
        "SELECT COUNT(*) AS CNT FROM FILING_FACTS WHERE TICKER = %s",
        (ticker.upper(),),
    )
    return rows[0]["CNT"] if rows else 0


def get_fy_shares_from_filing_facts(ticker: str, share_type: str = "diluted") -> list[dict]:
    """Get full-year (10-K) share counts from Cortex AI-extracted filing facts.

    Note: Cortex AI sometimes extracts values "in thousands" (matching SEC
    filing units) rather than actual shares.  The caller (derive_q4_from_fy)
    auto-corrects this by comparing against quarterly XBRL reference values.

    Args:
        ticker: Company ticker symbol.
        share_type: "diluted" or "basic" — which column to read.

    Returns list of dicts with keys: fy_end (str), fy_shares (int).
    """
    col = "DILUTED_SHARES_REPORTED" if share_type == "diluted" else "BASIC_SHARES_REPORTED"
    rows = _execute(
        f"SELECT PERIOD_END, {col} AS FY_SHARES "
        f"FROM FILING_FACTS "
        f"WHERE TICKER = %s AND FORM_TYPE = '10-K' AND {col} IS NOT NULL "
        f"ORDER BY PERIOD_END",
        (ticker.upper(),),
    )
    return [{"fy_end": str(r["PERIOD_END"]), "fy_shares": int(r["FY_SHARES"])} for r in rows]


def get_q4_eps_data(ticker: str) -> list[dict]:
    """Get Q4 net income and EPS from 10-K filings for back-calculating Q4 shares.

    Uses the Morgan Stanley methodology: Q4 diluted shares = |Q4 net income| / |Q4 EPS|.
    Only returns rows where both Q4_NET_INCOME and Q4_EPS are non-null and EPS != 0.

    Returns list of dicts with keys: fy_end (str), q4_net_income (int), q4_eps (float).
    """
    rows = _execute(
        "SELECT PERIOD_END, Q4_NET_INCOME, Q4_EPS "
        "FROM FILING_FACTS "
        "WHERE TICKER = %s AND FORM_TYPE = '10-K' "
        "  AND Q4_NET_INCOME IS NOT NULL AND Q4_EPS IS NOT NULL "
        "  AND Q4_EPS != 0 "
        "ORDER BY PERIOD_END",
        (ticker.upper(),),
    )
    return [
        {
            "fy_end": str(r["PERIOD_END"]),
            "q4_net_income": int(r["Q4_NET_INCOME"]),
            "q4_eps": float(r["Q4_EPS"]),
        }
        for r in rows
    ]


def get_filing_facts_quarterly(ticker: str, share_type: str = "diluted") -> list[dict]:
    """Get quarterly (10-Q) share counts from Cortex AI-extracted FILING_FACTS.

    Used to backfill gaps where XBRL extraction failed.  Returns dicts shaped
    like DILUTED_SHARES / BASIC_SHARES rows so they can be merged directly
    into the raw rows list before compute_dilution_metrics runs.

    The FORM_TYPE is set to '10-Q (AI)' to distinguish from XBRL-extracted rows.

    Args:
        ticker: Company ticker symbol.
        share_type: "diluted" or "basic" — which column to read.

    Returns list of dicts with uppercase keys matching the DILUTED_SHARES /
    BASIC_SHARES table schema.
    """
    share_col = "DILUTED_SHARES_REPORTED" if share_type == "diluted" else "BASIC_SHARES_REPORTED"
    out_col = "DILUTED_SHARES" if share_type == "diluted" else "BASIC_SHARES"

    # Detect fiscal-year-end month from the 10-K period_end for this ticker.
    fye_rows = _execute(
        "SELECT MONTH(ff.PERIOD_END) AS FYE_MONTH "
        "FROM DILUTION_MONITOR.PUBLIC.FILING_FACTS ff "
        "WHERE ff.TICKER = %s AND ff.FORM_TYPE = '10-K' "
        "LIMIT 1",
        (ticker.upper(),),
    )
    fye_month = fye_rows[0]["FYE_MONTH"] if fye_rows else 12  # default Dec

    rows = _execute(
        f"SELECT ff.TICKER, ff.PERIOD_END, ff.{share_col} AS SHARE_VALUE, "
        f"       ff.ACCESSION_NUMBER, "
        f"       f.FILED_DATE, f.CIK, f.ENTITY_NAME "
        f"FROM DILUTION_MONITOR.PUBLIC.FILING_FACTS ff "
        f"JOIN SEC_FILINGS.PUBLIC.FILINGS f "
        f"  ON ff.TICKER = f.TICKER AND ff.ACCESSION_NUMBER = f.ACCESSION_NUMBER "
        f"WHERE ff.TICKER = %s "
        f"  AND ff.FORM_TYPE = '10-Q' "
        f"  AND ff.{share_col} IS NOT NULL "
        f"ORDER BY ff.PERIOD_END",
        (ticker.upper(),),
    )

    # Build quarter lookup: months after FYE → fiscal quarter label.
    # E.g. FYE=Jan(1): Apr→Q1, Jul→Q2, Oct→Q3
    _quarter_map: dict[int, str] = {}
    for qi in range(1, 4):  # Q1, Q2, Q3 only (Q4 = FYE month, which is 10-K)
        m = (fye_month + qi * 3) % 12 or 12
        _quarter_map[m] = f"Q{qi}"

    result = []
    for r in rows:
        pe = r["PERIOD_END"]
        pe_month = pe.month if hasattr(pe, "month") else None
        pe_year = pe.year if hasattr(pe, "year") else None

        # Derive fiscal year: FY ends in fye_month.  Quarters before
        # the FYE month belong to the FY that ends in the *next* calendar year.
        if pe_month is not None and pe_year is not None:
            if pe_month <= fye_month:
                fiscal_year = pe_year
            else:
                fiscal_year = pe_year + 1
            fiscal_period = _quarter_map.get(pe_month)
        else:
            fiscal_year = None
            fiscal_period = None

        result.append({
            "TICKER": r["TICKER"],
            "CIK": r.get("CIK", ""),
            "ENTITY_NAME": r.get("ENTITY_NAME", ""),
            "PERIOD_START": None,
            "PERIOD_END": pe,
            "FISCAL_YEAR": fiscal_year,
            "FISCAL_PERIOD": fiscal_period,
            "FORM_TYPE": "10-Q (Cortex)",
            out_col: int(r["SHARE_VALUE"]),
            "FILED_DATE": r.get("FILED_DATE"),
            "ACCESSION_NUMBER": r.get("ACCESSION_NUMBER"),
            "FRAME": None,
        })
    return result


# ---------------------------------------------------------------------------
# Diluted shares
# ---------------------------------------------------------------------------


def insert_diluted_shares(records: list[dict]) -> int:
    """Insert diluted shares records, skipping duplicates by (TICKER, FRAME)."""
    inserted = 0
    for rec in records:
        try:
            rows = _execute_no_fetch(
                "INSERT INTO DILUTED_SHARES "
                "(TICKER, CIK, ENTITY_NAME, PERIOD_START, PERIOD_END, "
                "FISCAL_YEAR, FISCAL_PERIOD, FORM_TYPE, DILUTED_SHARES, "
                "FILED_DATE, ACCESSION_NUMBER, FRAME) "
                "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM DILUTED_SHARES WHERE TICKER = %s AND FRAME = %s"
                ")",
                (
                    rec["ticker"], rec["cik"], rec.get("entity_name", ""),
                    rec.get("period_start"), rec["period_end"],
                    rec.get("fiscal_year"), rec.get("fiscal_period"),
                    rec.get("form_type"), rec["diluted_shares"],
                    rec.get("filed_date"), rec.get("accession_number"),
                    rec["frame"],
                    rec["ticker"], rec["frame"],
                ),
            )
            inserted += rows
        except Exception:
            pass  # duplicate
    return inserted


def get_diluted_shares(ticker: str) -> list[dict]:
    """Get all diluted shares data for a ticker, ordered by period."""
    return _execute(
        "SELECT * FROM DILUTED_SHARES WHERE TICKER = %s "
        "ORDER BY PERIOD_END",
        (ticker.upper(),),
    )


# ---------------------------------------------------------------------------
# Basic shares
# ---------------------------------------------------------------------------


def insert_basic_shares(records: list[dict]) -> int:
    """Insert basic shares records, skipping duplicates by (TICKER, FRAME)."""
    inserted = 0
    for rec in records:
        try:
            rows = _execute_no_fetch(
                "INSERT INTO BASIC_SHARES "
                "(TICKER, CIK, ENTITY_NAME, PERIOD_END, "
                "FISCAL_YEAR, FISCAL_PERIOD, FORM_TYPE, BASIC_SHARES, "
                "FILED_DATE, ACCESSION_NUMBER, FRAME) "
                "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM BASIC_SHARES WHERE TICKER = %s AND FRAME = %s"
                ")",
                (
                    rec["ticker"], rec["cik"], rec.get("entity_name", ""),
                    rec["period_end"],
                    rec.get("fiscal_year"), rec.get("fiscal_period"),
                    rec.get("form_type"), rec["basic_shares"],
                    rec.get("filed_date"), rec.get("accession_number"),
                    rec["frame"],
                    rec["ticker"], rec["frame"],
                ),
            )
            inserted += rows
        except Exception:
            pass
    return inserted


def get_basic_shares(ticker: str) -> list[dict]:
    """Get all basic shares data for a ticker, ordered by period."""
    return _execute(
        "SELECT * FROM BASIC_SHARES WHERE TICKER = %s "
        "ORDER BY PERIOD_END",
        (ticker.upper(),),
    )


# ---------------------------------------------------------------------------
# Buyback activity
# ---------------------------------------------------------------------------


def insert_buyback_activity(records: list[dict]) -> int:
    """Insert buyback activity records, skipping duplicates by (TICKER, FRAME)."""
    inserted = 0
    for rec in records:
        try:
            rows = _execute_no_fetch(
                "INSERT INTO BUYBACK_ACTIVITY "
                "(TICKER, CIK, PERIOD_START, PERIOD_END, "
                "FISCAL_YEAR, FISCAL_PERIOD, SHARES_REPURCHASED, "
                "REPURCHASE_VALUE_USD, FORM_TYPE, FILED_DATE, "
                "ACCESSION_NUMBER, FRAME) "
                "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM BUYBACK_ACTIVITY WHERE TICKER = %s AND FRAME = %s"
                ")",
                (
                    rec["ticker"], rec["cik"],
                    rec.get("period_start"), rec["period_end"],
                    rec.get("fiscal_year"), rec.get("fiscal_period"),
                    rec.get("shares_repurchased", 0),
                    rec.get("repurchase_value_usd", 0),
                    rec.get("form_type"), rec.get("filed_date"),
                    rec.get("accession_number"), rec["frame"],
                    rec["ticker"], rec["frame"],
                ),
            )
            inserted += rows
        except Exception:
            pass
    return inserted


def get_buyback_activity(ticker: str) -> list[dict]:
    """Get all buyback activity for a ticker, ordered by period."""
    return _execute(
        "SELECT * FROM BUYBACK_ACTIVITY WHERE TICKER = %s "
        "ORDER BY PERIOD_END",
        (ticker.upper(),),
    )


# ---------------------------------------------------------------------------
# Buyback programs (authorization amounts)
# ---------------------------------------------------------------------------


def insert_buyback_programs(records: list[dict]) -> int:
    """Insert buyback program records, skipping duplicates by (TICKER, FRAME)."""
    inserted = 0
    for rec in records:
        try:
            rows = _execute_no_fetch(
                "INSERT INTO BUYBACK_PROGRAMS "
                "(TICKER, CIK, PERIOD_END, AUTHORIZED_AMOUNT_USD, "
                "FORM_TYPE, FILED_DATE, ACCESSION_NUMBER, FRAME) "
                "SELECT %s,%s,%s,%s,%s,%s,%s,%s "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM BUYBACK_PROGRAMS WHERE TICKER = %s AND FRAME = %s"
                ")",
                (
                    rec["ticker"], rec["cik"], rec["period_end"],
                    rec["authorized_amount_usd"],
                    rec.get("form_type"), rec.get("filed_date"),
                    rec.get("accession_number"), rec["frame"],
                    rec["ticker"], rec["frame"],
                ),
            )
            inserted += rows
        except Exception:
            pass
    return inserted


def get_buyback_programs(ticker: str) -> list[dict]:
    """Get all buyback program authorizations for a ticker."""
    return _execute(
        "SELECT * FROM BUYBACK_PROGRAMS WHERE TICKER = %s "
        "ORDER BY PERIOD_END",
        (ticker.upper(),),
    )


# ---------------------------------------------------------------------------
# Ingestion queue (cross-app)
# ---------------------------------------------------------------------------


def enqueue_ingestions(ticker: str, requested_by: str) -> int:
    """Insert PENDING ingestion requests for all registered monitors.

    Reads WATCHLIST_HUB.PUBLIC.MONITOR_REGISTRY to discover monitors, then
    inserts one PENDING row per monitor into INGESTION_QUEUE (skipping if a
    PENDING row already exists for that ticker+monitor combo).

    Returns the number of rows inserted.
    """
    monitors = _execute(
        "SELECT MONITOR_ID FROM WATCHLIST_HUB.PUBLIC.MONITOR_REGISTRY "
        "WHERE ACTIVE = TRUE"
    )
    inserted = 0
    for m in monitors:
        monitor_key = m["MONITOR_ID"]
        _execute_no_fetch(
            "INSERT INTO WATCHLIST_HUB.PUBLIC.INGESTION_QUEUE "
            "(TICKER, MONITOR, STATUS, REQUESTED_BY) "
            "SELECT %s, %s, 'PENDING', %s "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM WATCHLIST_HUB.PUBLIC.INGESTION_QUEUE "
            "  WHERE TICKER = %s AND MONITOR = %s AND STATUS = 'PENDING'"
            ")",
            (ticker.upper(), monitor_key, requested_by,
             ticker.upper(), monitor_key),
        )
        inserted += 1
    return inserted


def claim_pending_ingestions(monitor: str) -> list[dict]:
    """Claim all PENDING queue rows for a given monitor.

    Sets STATUS = 'RUNNING' and STARTED_AT = now, then returns the claimed rows.
    """
    now = datetime.now(timezone.utc)
    _execute_no_fetch(
        "UPDATE WATCHLIST_HUB.PUBLIC.INGESTION_QUEUE "
        "SET STATUS = 'RUNNING', STARTED_AT = %s "
        "WHERE MONITOR = %s AND STATUS = 'PENDING'",
        (now, monitor),
    )
    return _execute(
        "SELECT ID, TICKER, MONITOR, REQUESTED_BY, REQUESTED_AT "
        "FROM WATCHLIST_HUB.PUBLIC.INGESTION_QUEUE "
        "WHERE MONITOR = %s AND STATUS = 'RUNNING' AND STARTED_AT = %s "
        "ORDER BY REQUESTED_AT",
        (monitor, now),
    )


def complete_queued_ingestion(
    queue_id: int, status: str = "COMPLETED", error_message: Optional[str] = None,
):
    """Mark a queue row as COMPLETED or FAILED."""
    now = datetime.now(timezone.utc)
    _execute_no_fetch(
        "UPDATE WATCHLIST_HUB.PUBLIC.INGESTION_QUEUE "
        "SET STATUS = %s, COMPLETED_AT = %s, ERROR_MESSAGE = %s "
        "WHERE ID = %s",
        (status, now, error_message, queue_id),
    )


# ---------------------------------------------------------------------------
# Cortex AI filing extraction
# ---------------------------------------------------------------------------


def run_cortex_extraction(ticker: str) -> int:
    """Run Cortex AI extraction on SEC filings and insert results into FILING_FACTS.

    Processes all filings in SEC_FILINGS.PUBLIC.FILINGS for the given ticker
    that don't already have a FILING_FACTS row.  Uses targeted text windows
    (cover, shares-outstanding section, diluted section, split/events, end)
    and SNOWFLAKE.CORTEX.AI_COMPLETE with llama3.3-70b.

    Returns the number of new FILING_FACTS rows inserted.
    """
    sql = (
        "INSERT INTO DILUTION_MONITOR.PUBLIC.FILING_FACTS\n"
        "  (TICKER, FORM_TYPE, PERIOD_END, DILUTED_SHARES_REPORTED, BASIC_SHARES_REPORTED,\n"
        "   STOCK_SPLIT_RATIO, STOCK_SPLIT_DATE,\n"
        "   RAW_AI_RESPONSE, EXTRACTED_AT, ACCESSION_NUMBER)\n"
        "WITH windows AS (\n"
        "  SELECT\n"
        "    f.TICKER, f.CIK, f.ACCESSION_NUMBER, f.FORM_TYPE, f.REPORT_DATE, f.DOCUMENT_LENGTH,\n"
        "    LEFT(f.DOCUMENT_TEXT, 10000) AS w1,\n"
        "    CASE WHEN POSITION('shares outstanding' IN LOWER(f.DOCUMENT_TEXT)) > 0\n"
        "         THEN SUBSTR(f.DOCUMENT_TEXT, GREATEST(1, POSITION('shares outstanding' IN LOWER(f.DOCUMENT_TEXT)) - 2000), 8000)\n"
        "         ELSE '' END AS w2,\n"
        "    CASE WHEN POSITION('diluted' IN LOWER(f.DOCUMENT_TEXT)) > 0\n"
        "         THEN SUBSTR(f.DOCUMENT_TEXT, GREATEST(1, POSITION('diluted' IN LOWER(f.DOCUMENT_TEXT)) - 2000), 8000)\n"
        "         ELSE '' END AS w3,\n"
        "    CASE WHEN POSITION('stock split' IN LOWER(f.DOCUMENT_TEXT)) > 0\n"
        "         THEN SUBSTR(f.DOCUMENT_TEXT, GREATEST(1, POSITION('stock split' IN LOWER(f.DOCUMENT_TEXT)) - 3000), 10000)\n"
        "         WHEN POSITION('split' IN LOWER(f.DOCUMENT_TEXT)) > 0\n"
        "         THEN SUBSTR(f.DOCUMENT_TEXT, GREATEST(1, POSITION('split' IN LOWER(f.DOCUMENT_TEXT)) - 2000), 8000)\n"
        "         ELSE '' END AS w4,\n"
        "    RIGHT(f.DOCUMENT_TEXT, 10000) AS w5\n"
        "  FROM SEC_FILINGS.PUBLIC.FILINGS f\n"
        "  WHERE f.TICKER = %s\n"
        "    AND f.ACCESSION_NUMBER NOT IN (\n"
        "      SELECT ACCESSION_NUMBER FROM DILUTION_MONITOR.PUBLIC.FILING_FACTS WHERE TICKER = %s\n"
        "    )\n"
        "),\n"
        "extracted AS (\n"
        "  SELECT TICKER, ACCESSION_NUMBER, FORM_TYPE, REPORT_DATE,\n"
        "    SNOWFLAKE.CORTEX.AI_COMPLETE('llama3.3-70b',\n"
        "      'You are analyzing excerpts from a SEC filing. Extract these facts. Return ONLY valid JSON, no markdown.\\n'\n"
        "      || 'Fields:\\n'\n"
        "      || '- stock_splits: array of {ratio, effective_date}. Only actual stock splits declared or completed in this filing period. Empty array if none.\\n'\n"
        "      || '- diluted_shares: weighted average diluted shares for the most recent period. Full number (not in thousands). null if not found.\\n'\n"
        "      || '- basic_shares: basic/common shares outstanding as of reporting date. Full number. null if not found.\\n'\n"
        "      || 'IMPORTANT: If table header says \"in thousands\", multiply values by 1,000. If \"in millions\", multiply by 1,000,000.\\n'\n"
        "      || '--- COVER ---\\n' || w1\n"
        "      || '\\n--- SHARES OUTSTANDING ---\\n' || w2\n"
        "      || '\\n--- DILUTED SHARES ---\\n' || w3\n"
        "      || '\\n--- SPLIT/EVENTS ---\\n' || w4\n"
        "      || '\\n--- END ---\\n' || w5,\n"
        "      {'max_tokens': 500, 'temperature': 0}\n"
        "    ) AS ai_raw\n"
        "  FROM windows\n"
        "),\n"
        "parsed AS (\n"
        "  SELECT TICKER, ACCESSION_NUMBER, FORM_TYPE, REPORT_DATE, ai_raw,\n"
        "    TRY_PARSE_JSON(REPLACE(REPLACE(ai_raw, '```json', ''), '```', '')) AS j\n"
        "  FROM extracted\n"
        ")\n"
        "SELECT\n"
        "  TICKER, FORM_TYPE, REPORT_DATE,\n"
        "  j:diluted_shares::NUMBER AS diluted_shares_reported,\n"
        "  j:basic_shares::NUMBER AS basic_shares_reported,\n"
        "  CASE WHEN ARRAY_SIZE(j:stock_splits) > 0\n"
        "       THEN REPLACE(j:stock_splits[0]:ratio::VARCHAR, '-for-', ':')\n"
        "       ELSE NULL END AS stock_split_ratio,\n"
        "  CASE WHEN ARRAY_SIZE(j:stock_splits) > 0\n"
        "       THEN TRY_TO_DATE(j:stock_splits[0]:effective_date::VARCHAR)\n"
        "       ELSE NULL END AS stock_split_date,\n"
        "  ai_raw, CURRENT_TIMESTAMP(), ACCESSION_NUMBER\n"
        "FROM parsed\n"
    )
    try:
        rows = _execute_no_fetch(sql, (ticker.upper(), ticker.upper()))
        logger.info(f"Cortex extraction for {ticker}: {rows} filing facts inserted")
        return rows
    except Exception as e:
        logger.error(f"Cortex extraction failed for {ticker}: {e}")
        raise


def run_q4_extraction(ticker: str) -> int:
    """Run Cortex AI extraction of Q4 net income and EPS from 10-K filings.

    Finds the quarterly results section in 10-K filings and uses
    llama3.1-70b to extract Q4-specific financial data.  Updates
    existing FILING_FACTS rows with Q4_NET_INCOME and Q4_EPS.

    Returns the number of rows updated.
    """
    sql = (
        "UPDATE DILUTION_MONITOR.PUBLIC.FILING_FACTS ff\n"
        "SET\n"
        "  Q4_NET_INCOME = parsed.q4_net_income,\n"
        "  Q4_EPS = parsed.q4_eps\n"
        "FROM (\n"
        "  WITH quarterly_filings AS (\n"
        "    SELECT\n"
        "      ff2.TICKER,\n"
        "      ff2.ACCESSION_NUMBER,\n"
        "      ff2.PERIOD_END,\n"
        "      f.DOCUMENT_TEXT,\n"
        "      GREATEST(\n"
        "        COALESCE(POSITION('Quarterly Results' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('QUARTERLY RESULTS' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('Selected Quarterly' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('SELECTED QUARTERLY' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('Quarterly Financial Data' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('QUARTERLY FINANCIAL DATA' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('Supplemental Quarterly' IN f.DOCUMENT_TEXT), 0),\n"
        "        COALESCE(POSITION('SUPPLEMENTAL QUARTERLY' IN f.DOCUMENT_TEXT), 0)\n"
        "      ) AS section_pos\n"
        "    FROM DILUTION_MONITOR.PUBLIC.FILING_FACTS ff2\n"
        "    JOIN SEC_FILINGS.PUBLIC.FILINGS f\n"
        "      ON ff2.TICKER = f.TICKER AND ff2.ACCESSION_NUMBER = f.ACCESSION_NUMBER\n"
        "    WHERE ff2.FORM_TYPE = '10-K'\n"
        "      AND ff2.TICKER = %s\n"
        "      AND ff2.Q4_NET_INCOME IS NULL\n"
        "  ),\n"
        "  filings_with_section AS (\n"
        "    SELECT * FROM quarterly_filings WHERE section_pos > 0\n"
        "  ),\n"
        "  extraction AS (\n"
        "    SELECT\n"
        "      TICKER, ACCESSION_NUMBER,\n"
        "      SNOWFLAKE.CORTEX.COMPLETE(\n"
        "        'llama3.1-70b',\n"
        "        CONCAT(\n"
        "          'Extract Q4 quarterly financial data from this 10-K annual report section. ',\n"
        "          'The fiscal year ends on ', TO_CHAR(PERIOD_END, 'YYYY-MM-DD'), '. ',\n"
        "          'Q4 is the LAST quarter of the fiscal year (the three months ending on the fiscal year end date). ',\n"
        "          'Find: 1) Q4 net income or net loss in thousands of dollars. 2) Q4 diluted net income/loss per share (EPS). ',\n"
        "          'Losses should be NEGATIVE numbers. ',\n"
        "          'Return ONLY valid JSON: {\"q4_net_income_thousands\": <integer>, \"q4_diluted_eps\": <number>} ',\n"
        "          'If you cannot find these values, return: {\"q4_net_income_thousands\": null, \"q4_diluted_eps\": null} ',\n"
        "          'Here is the quarterly data section: ',\n"
        "          SUBSTR(DOCUMENT_TEXT, section_pos, 6000)\n"
        "        )\n"
        "      ) AS raw_result\n"
        "    FROM filings_with_section\n"
        "  ),\n"
        "  json_extracted AS (\n"
        "    SELECT TICKER, ACCESSION_NUMBER, raw_result,\n"
        r"      REGEXP_SUBSTR(raw_result, '\\{[^{}]*\"q4_net_income_thousands\"[^{}]*\\}') AS json_str"
        "\n"
        "    FROM extraction\n"
        "  )\n"
        "  SELECT\n"
        "    TICKER, ACCESSION_NUMBER,\n"
        "    TRY_CAST(PARSE_JSON(json_str):q4_net_income_thousands::VARCHAR AS INTEGER) AS q4_net_income,\n"
        "    TRY_CAST(PARSE_JSON(json_str):q4_diluted_eps::VARCHAR AS FLOAT) AS q4_eps\n"
        "  FROM json_extracted\n"
        "  WHERE json_str IS NOT NULL\n"
        ") parsed\n"
        "WHERE ff.TICKER = parsed.TICKER\n"
        "  AND ff.ACCESSION_NUMBER = parsed.ACCESSION_NUMBER\n"
        "  AND parsed.q4_net_income IS NOT NULL\n"
        "  AND parsed.q4_eps IS NOT NULL\n"
    )
    try:
        rows = _execute_no_fetch(sql, (ticker.upper(),))
        logger.info(f"Q4 extraction for {ticker}: {rows} filing facts updated")
        return rows
    except Exception as e:
        logger.error(f"Q4 extraction failed for {ticker}: {e}")
        raise


def _get_github_pat() -> Optional[str]:
    """Read GitHub PAT from Streamlit secrets or environment variable."""
    try:
        import streamlit as st
        pat = st.secrets.get("github", {}).get("pat")
        if pat:
            return pat
    except Exception:
        pass
    return os.environ.get("GH_DISPATCH_PAT")


def trigger_cross_app_ingestion(ticker: str, source_monitor: str):
    """Dispatch GitHub Actions workflows for all other registered monitors.

    Reads MONITOR_REGISTRY for workflow metadata, then fires a
    workflow_dispatch event for each monitor except the caller.
    Falls back silently — the INGESTION_QUEUE serves as backup.
    """
    import requests

    pat = _get_github_pat()
    if not pat:
        logger.warning("GH_DISPATCH_PAT not configured — skipping cross-app dispatch")
        return

    monitors = _execute(
        "SELECT MONITOR_ID, GITHUB_REPO, WORKFLOW_FILE, GITHUB_REF "
        "FROM WATCHLIST_HUB.PUBLIC.MONITOR_REGISTRY "
        "WHERE ACTIVE = TRUE AND MONITOR_ID != %s",
        (source_monitor,),
    )

    for m in monitors:
        repo = m["GITHUB_REPO"]
        workflow = m["WORKFLOW_FILE"]
        ref = m["GITHUB_REF"] or "main"
        url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
        try:
            resp = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {pat}",
                    "Accept": "application/vnd.github.v3+json",
                },
                json={"ref": ref, "inputs": {"tickers": ticker.upper()}},
                timeout=10,
            )
            if resp.status_code == 204:
                logger.info(f"Dispatched {workflow} for {ticker} on {repo}")
            else:
                logger.warning(
                    f"GitHub dispatch failed for {repo}: "
                    f"{resp.status_code} {resp.text[:200]}"
                )
        except Exception as e:
            logger.warning(f"GitHub dispatch error for {repo}: {e}")

