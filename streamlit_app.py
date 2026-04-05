"""Streamlit frontend for Share Dilution Monitor.

Search-first UI: users enter a ticker, data is fetched from Snowflake
(or auto-ingested from SEC EDGAR on first search). Results displayed
via Altair charts and summary metrics.

Deployable to Streamlit Community Cloud.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd
import altair as alt
import logging

from services import snowflake_dilution as sf
from services import sec_xbrl as edgar
from services import analysis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Page configuration ---

st.set_page_config(
    page_title="Share Dilution Monitor",
    page_icon="📉",
    layout="wide",
)

st.title("Share Dilution Monitor")

page = st.sidebar.radio(
    "Navigation",
    ["Search", "Watchlist Overview"],
)

# --- Ensure local watchlist table exists ---
sf.ensure_dilution_watchlist_table()

# --- Sidebar: Manage Watchlist ---
st.sidebar.divider()
st.sidebar.subheader("Manage Watchlist")

new_ticker = st.sidebar.text_input(
    "Ticker symbol", placeholder="e.g. MSFT", key="add_ticker_input"
).strip().upper()

if st.sidebar.button("Add to Watchlist", use_container_width=True):
    if not new_ticker:
        st.sidebar.error("Enter a ticker symbol.")
    else:
        with st.sidebar:
            with st.spinner(f"Looking up {new_ticker}..."):
                company = edgar.resolve_ticker_to_cik(new_ticker)
            if not company:
                st.error(f"Could not find '{new_ticker}' on SEC EDGAR.")
            else:
                added = sf.add_to_dilution_watchlist(
                    ticker=company["ticker"],
                    company_name=company["name"],
                    cik=company["cik"],
                    exchange=company.get("exchange", ""),
                )
                if added:
                    st.success(f"Added {company['name']} ({company['ticker']})")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.warning(f"{company['ticker']} is already on the watchlist.")

# Display current local watchlist with remove buttons
local_watchlist = sf.get_dilution_watchlist()
if local_watchlist:
    st.sidebar.caption(f"{len(local_watchlist)} company(ies) on your watchlist")
    for entry in local_watchlist:
        col_name, col_btn = st.sidebar.columns([3, 1])
        col_name.markdown(f"**{entry['TICKER']}**")
        if col_btn.button("X", key=f"rm_{entry['TICKER']}", help=f"Remove {entry['TICKER']}"):
            sf.remove_from_dilution_watchlist(entry["TICKER"])
            st.cache_data.clear()
            st.rerun()
else:
    st.sidebar.caption("Your watchlist is empty. Add a ticker above.")

st.sidebar.divider()


# --- Helper: format large numbers ---

def fmt_shares(n):
    """Format share counts into human-readable form."""
    if n is None:
        return "-"
    n = float(n)
    if abs(n) >= 1e9:
        return f"{n / 1e9:,.2f}B"
    if abs(n) >= 1e6:
        return f"{n / 1e6:,.1f}M"
    if abs(n) >= 1e3:
        return f"{n / 1e3:,.0f}K"
    return f"{n:,.0f}"


def fmt_currency(n):
    """Format dollar amounts into human-readable form."""
    if n is None:
        return "-"
    n = float(n)
    if abs(n) >= 1e9:
        return f"${n / 1e9:,.2f}B"
    if abs(n) >= 1e6:
        return f"${n / 1e6:,.1f}M"
    if abs(n) >= 1e3:
        return f"${n / 1e3:,.0f}K"
    return f"${n:,.0f}"


def fmt_pct(n):
    """Format percentage."""
    if n is None or pd.isna(n):
        return "-"
    return f"{n:+.2f}%"


# --- Helper: ingest a company ---

def ingest_company(ticker: str) -> bool:
    """Resolve ticker, fetch EDGAR data, store in Snowflake. Returns True on success."""
    company = edgar.resolve_ticker_to_cik(ticker)
    if not company:
        st.error(f"Could not resolve ticker '{ticker}' via SEC EDGAR.")
        return False

    cik = company["cik"]
    entity_name = company["name"]
    ipo_date = company.get("ipo_date")

    try:
        data = edgar.ingest_company(cik, ticker.upper())

        sf.insert_diluted_shares(data["diluted_shares"])
        sf.insert_basic_shares(data["basic_shares"])
        sf.insert_buyback_activity(data["buyback_activity"])
        sf.insert_buyback_programs(data["buyback_programs"])

        total_points = (
            len(data["diluted_shares"])
            + len(data["basic_shares"])
            + len(data["buyback_activity"])
            + len(data["buyback_programs"])
        )

        sf.upsert_ingestion_log(
            ticker.upper(), cik, entity_name,
            status="SUCCESS", ipo_date=ipo_date,
        )
        st.success(
            f"Ingested {total_points} data points for {entity_name} ({ticker.upper()})"
        )
        return True

    except Exception as e:
        sf.upsert_ingestion_log(
            ticker.upper(), cik, entity_name,
            status="FAILED", error_message=str(e)[:2000],
        )
        st.error(f"Ingestion failed: {e}")
        return False


# ============================================================
# SEARCH PAGE
# ============================================================

if page == "Search":

    # Build dropdown options from watchlist + already-ingested companies
    @st.cache_data(ttl=300)
    def _load_company_options():
        """Merge watchlist and ingested tickers into a deduplicated list."""
        options = {}  # ticker -> display name

        for row in sf.get_insider_watchlist():
            t = row["TICKER"]
            options[t] = f"{t} — {row['COMPANY_NAME']}"

        for row in sf.get_dilution_watchlist():
            t = row["TICKER"]
            if t not in options:
                options[t] = f"{t} — {row['COMPANY_NAME']}"

        for row in sf.get_all_ingested_tickers():
            t = row["TICKER"]
            if t not in options:
                options[t] = f"{t} — {row['ENTITY_NAME']}"

        return dict(sorted(options.items()))

    company_options = _load_company_options()
    option_labels = list(company_options.values())
    option_tickers = list(company_options.keys())

    st.markdown(
        "#### I want to learn more about"
    )

    selected_idx = st.selectbox(
        "Company",
        range(len(option_tickers)),
        format_func=lambda i: option_labels[i],
        label_visibility="collapsed",
    )

    ticker_input = option_tickers[selected_idx] if option_tickers else ""

    if ticker_input:
        # Check if we already have data
        ingestion = sf.get_ingestion_status(ticker_input)

        if not ingestion or ingestion.get("STATUS") == "FAILED":
            with st.spinner(
                f"Fetching data for {ticker_input} from SEC EDGAR... "
                f"(this may take a moment for the first time)"
            ):
                success = ingest_company(ticker_input)
            if not success:
                st.stop()
            ingestion = sf.get_ingestion_status(ticker_input)

        # Load data from Snowflake
        diluted_rows = sf.get_diluted_shares(ticker_input)
        basic_rows = sf.get_basic_shares(ticker_input)
        buyback_rows = sf.get_buyback_activity(ticker_input)
        program_rows = sf.get_buyback_programs(ticker_input)

        # Backfill missing quarters from Cortex AI-extracted FILING_FACTS.
        # This injects AI rows into the raw lists BEFORE split/scale correction,
        # so adjust_for_splits auto-corrects any *1000 scale issues.
        ai_diluted = sf.get_filing_facts_quarterly(ticker_input, share_type="diluted")
        ai_basic = sf.get_filing_facts_quarterly(ticker_input, share_type="basic")
        diluted_rows = analysis.backfill_from_filing_facts(diluted_rows, ai_diluted, share_key="DILUTED_SHARES")
        basic_rows = analysis.backfill_from_filing_facts(basic_rows, ai_basic, share_key="BASIC_SHARES")

        # Filter raw rows to post-IPO only BEFORE analysis (so split detection
        # doesn't misinterpret IPO-related share jumps as stock splits)
        ipo_date = sf.get_ipo_date(ticker_input)
        if ipo_date:
            diluted_rows = [r for r in diluted_rows if str(r.get("PERIOD_END", "")) >= ipo_date]
            basic_rows = [r for r in basic_rows if str(r.get("PERIOD_END", "")) >= ipo_date]
            buyback_rows = [r for r in buyback_rows if str(r.get("PERIOD_END", "")) >= ipo_date]
            program_rows = [r for r in program_rows if str(r.get("PERIOD_END", "")) >= ipo_date]

        if not diluted_rows and not basic_rows:
            st.warning(
                f"No share data found for {ticker_input}. "
                f"This company may not report via standard XBRL tags."
            )
            st.stop()

        # Use verified splits from Cortex AI filing extraction when available.
        # Only fall back to heuristic detection when no filing facts exist
        # (i.e. extraction hasn't been run for this ticker yet).
        filing_facts_count = sf.get_filing_facts_count(ticker_input)
        verified_splits = sf.get_verified_splits(ticker_input) if filing_facts_count > 0 else None

        if verified_splits is not None:
            # Cortex AI extraction ran — trust its results (even if empty = no splits)
            dilution_df, dilution_splits = analysis.compute_dilution_metrics(
                diluted_rows, known_splits=verified_splits,
            )
            basic_df, basic_splits = analysis.compute_basic_shares_metrics(
                basic_rows, known_splits=verified_splits,
            )
            split_events = verified_splits
        else:
            # No filing facts — fall back to heuristic split detection
            dilution_df, dilution_splits = analysis.compute_dilution_metrics(diluted_rows)
            basic_df, basic_splits = analysis.compute_basic_shares_metrics(
                basic_rows, known_splits=dilution_splits or None,
            )
            split_events = dilution_splits or basic_splits
        buyback_df = analysis.compute_buyback_metrics(
            buyback_rows, program_rows, split_events=split_events,
        )

        # --- Build a single primary shares series ---
        # Prefer diluted shares; fall back to basic when diluted is unavailable
        # (e.g. MDB only reports basic shares via XBRL).
        if not dilution_df.empty:
            primary_df = dilution_df.copy()
            primary_label = "Fully Diluted Shares Outstanding"
            using_basic_fallback = False
        elif not basic_df.empty:
            primary_df = basic_df.rename(columns={"basic_shares": "diluted_shares"}).copy()
            primary_label = "Basic Shares Outstanding"
            using_basic_fallback = True
        else:
            primary_df = pd.DataFrame()
            primary_label = "Shares Outstanding"
            using_basic_fallback = False

        # --- Derive Q4 data points from 10-K full-year figures ---
        # Q4 is not filed via 10-Q (only 10-K covers the FY-end quarter).
        # Two methods tried in order:
        #   1) MS methodology: Q4 = 4×FY − Q1 − Q2 − Q3 (needs 3 prior quarters)
        #   2) EPS fallback: Q4 shares = |Q4 net income| / |Q4 EPS| (from 10-K quarterly data)
        if not primary_df.empty and filing_facts_count > 0:
            share_type = "basic" if using_basic_fallback else "diluted"
            fy_shares = sf.get_fy_shares_from_filing_facts(ticker_input, share_type=share_type)
            derived_q4 = analysis.derive_q4_from_fy(
                primary_df, fy_shares, share_col="diluted_shares",
                known_splits=split_events or None,
            )
            if not derived_q4.empty:
                primary_df["is_derived"] = primary_df["form_type"].str.contains("AI|Cortex|Derived", case=False, na=False)
                primary_df = pd.concat([primary_df, derived_q4], ignore_index=True)
                primary_df = primary_df.sort_values("period_end").reset_index(drop=True)

            # EPS-based fallback for FY periods where derive_q4_from_fy couldn't
            # produce a Q4 (e.g. IPO-year with < 3 prior quarters).
            q4_eps_data = sf.get_q4_eps_data(ticker_input)
            if q4_eps_data:
                derived_eps = analysis.derive_q4_from_eps(
                    primary_df, q4_eps_data, share_col="diluted_shares",
                    known_splits=split_events or None,
                )
                if not derived_eps.empty:
                    primary_df["is_derived"] = primary_df["form_type"].str.contains("AI|Cortex|Derived", case=False, na=False)
                    primary_df = pd.concat([primary_df, derived_eps], ignore_index=True)
                    primary_df = primary_df.sort_values("period_end").reset_index(drop=True)

            # Post-derivation outlier cleanup: catch any remaining split or
            # scale outliers introduced by derived Q4 points.
            primary_df = analysis._fix_split_outliers(
                primary_df, "diluted_shares", split_events or [],
            )
            primary_df = analysis._fix_scale_outliers(
                primary_df, "diluted_shares",
            )

            # Recompute QoQ changes with all derived Q4 points included
            primary_df["is_derived"] = primary_df["form_type"].str.contains("AI|Cortex|Derived", case=False, na=False)
            primary_df["qoq_change"] = primary_df["diluted_shares"].diff()
            primary_df["qoq_change_pct"] = primary_df["diluted_shares"].pct_change() * 100
        else:
            if not primary_df.empty:
                primary_df["is_derived"] = primary_df["form_type"].str.contains("AI|Cortex|Derived", case=False, na=False)

        summary = analysis.compute_summary(
            primary_df if not primary_df.empty else dilution_df,
            basic_df,
            buyback_df,
        )

        entity_name = ingestion.get("ENTITY_NAME", ticker_input) if ingestion else ticker_input
        st.header(f"{entity_name} ({ticker_input})")

        # Show split adjustment notice
        if split_events:
            split_desc = ", ".join(
                f"{s['ratio']}:1 ({pd.to_datetime(s['date']).strftime('%b %Y')})"
                for s in split_events
            )
            st.info(
                f"Adjusted for {len(split_events)} stock split(s): {split_desc}. "
                f"All share counts are normalized to the current split-adjusted basis."
            )

        # --- Summary metrics ---
        share_type_label = "Basic Shares" if using_basic_fallback else "Diluted Shares"
        cols = st.columns(3)
        cols[0].metric(
            f"Latest {share_type_label}",
            fmt_shares(summary.get("latest_diluted_shares")),
        )
        cols[1].metric(
            "Total Change",
            fmt_shares(summary.get("total_dilution_change")),
            delta=fmt_pct(summary.get("total_dilution_pct")),
        )
        cols[2].metric(
            "Quarters Tracked",
            summary.get("total_periods", 0),
        )
        if using_basic_fallback:
            st.caption("⚠ This company reports a combined \"basic and diluted\" share count (diluted = basic due to net losses under GAAP anti-dilution rules). Showing basic shares outstanding.")

        if summary.get("latest_period"):
            st.caption(f"Latest data: {summary['latest_period']}")

        st.divider()

        # --- Primary Shares Chart ---
        if not primary_df.empty:
            st.subheader(f"{primary_label} Over Time")

            # Convert to thousands for display
            primary_df["diluted_shares_k"] = primary_df["diluted_shares"] / 1000

            # Clean source label for tooltips
            primary_df["source"] = primary_df["form_type"].apply(
                lambda x: "Cortex Extraction" if any(k in str(x).upper() for k in ("AI", "CORTEX"))
                else ("Derived" if "derived" in str(x).lower() else "XBRL")
            )

            x_enc = alt.X(
                "period_end:T",
                title="Quarter End",
                axis=alt.Axis(
                    format="%b %Y",
                    labelAngle=-45,
                    tickCount={"interval": "month", "step": 3},
                    grid=True,
                    gridOpacity=0.15,
                ),
            )
            y_enc = alt.Y(
                "diluted_shares_k:Q",
                title=f"{share_type_label} (Thousands)",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format=",.0f"),
            )
            tooltip_enc = [
                alt.Tooltip("period_end:T", title="Period", format="%b %Y"),
                alt.Tooltip("diluted_shares_k:Q", title=f"{share_type_label} (K)", format=",.0f"),
                alt.Tooltip("fiscal_year:O", title="Fiscal Year"),
                alt.Tooltip("fiscal_period:N", title="Quarter"),
                alt.Tooltip("source:N", title="Source"),
            ]

            area = (
                alt.Chart(primary_df)
                .mark_area(
                    line={"color": "#1E88E5"},
                    color=alt.Gradient(
                        gradient="linear",
                        stops=[
                            alt.GradientStop(color="#1E88E5", offset=1),
                            alt.GradientStop(color="rgba(30,136,229,0.08)", offset=0),
                        ],
                        x1=1, x2=1, y1=1, y2=0,
                    ),
                )
                .encode(x=x_enc, y=y_enc)
            )

            has_derived = "is_derived" in primary_df.columns and primary_df["is_derived"].any()

            # Build points layer with distinct markers for reported vs derived
            # XBRL and Cortex Extraction are both reported values — only Derived is different.
            primary_df["point_type"] = primary_df["source"].apply(
                lambda x: "Derived" if x == "Derived" else "Reported"
            )

            shape_domain = ["Reported", "Derived"]
            shape_range = ["circle", "triangle-up"]
            color_domain = ["Reported", "Derived"]
            color_range = ["#1E88E5", "#AB47BC"]

            if has_derived:
                derived_df = primary_df[primary_df["is_derived"]]

                points = (
                    alt.Chart(primary_df)
                    .mark_point(size=50, filled=True, strokeWidth=1.5)
                    .encode(
                        x=x_enc,
                        y=y_enc,
                        shape=alt.Shape(
                            "point_type:N",
                            title="Data Point Type",
                            scale=alt.Scale(domain=shape_domain, range=shape_range),
                        ),
                        color=alt.Color(
                            "point_type:N",
                            title="Data Point Type",
                            scale=alt.Scale(domain=color_domain, range=color_range),
                            legend=None,
                        ),
                        tooltip=tooltip_enc,
                    )
                )
                chart = alt.layer(area, points).properties(height=420)
            else:
                points = (
                    alt.Chart(primary_df)
                    .mark_circle(size=40, color="#1E88E5")
                    .encode(x=x_enc, y=y_enc, tooltip=tooltip_enc)
                )
                chart = alt.layer(area, points).properties(height=420)

            st.altair_chart(chart, use_container_width=True)

            # Methodology note
            derived_note = ""
            if has_derived:
                n_ai = int(derived_df["form_type"].str.contains("AI|Cortex", case=False, na=False).sum())
                n_q4_formula = int((derived_df["form_type"] == "Derived").sum())
                n_q4_eps = int(derived_df["form_type"].str.contains("Derived.*EPS", case=False, na=False).sum())
                parts = []
                if n_ai > 0:
                    parts.append(
                        f"{n_ai} quarter(s) backfilled from Cortex Extraction"
                    )
                if n_q4_formula > 0:
                    parts.append(
                        f"{n_q4_formula} Q4 point(s) derived via Q4 = 4 × FY − Q1 − Q2 − Q3"
                    )
                if n_q4_eps > 0:
                    parts.append(
                        f"{n_q4_eps} Q4 point(s) derived via |Net Income| / |EPS|"
                    )
                n_total = n_ai + n_q4_formula + n_q4_eps
                derived_note = (
                    f" {n_total} non-XBRL data point(s): "
                    + "; ".join(parts) + "."
                    + " See legend for marker shapes."
                )
            if using_basic_fallback:
                st.caption(
                    "**Methodology:** Each solid data point (●) is the weighted-average basic shares "
                    "outstanding as reported in the company's 10-Q filings via XBRL. "
                    "Diluted share data is not available through this company's structured "
                    "XBRL tags. Basic shares represent the actual common shares outstanding "
                    "before accounting for stock options, RSUs, and other dilutive securities."
                    + derived_note
                )
            else:
                st.caption(
                    "**Methodology:** Each solid data point (●) is the weighted-average fully diluted "
                    "shares outstanding as reported in the company's quarterly 10-Q filings "
                    "via SEC EDGAR XBRL. Diluted shares include common stock plus all "
                    "potentially dilutive securities (stock options, RSUs, convertible notes)."
                    + derived_note
                )

            # QoQ change bar chart
            change_df = primary_df.dropna(subset=["qoq_change"]).copy()
            if not change_df.empty:
                change_df["qoq_change_k"] = change_df["qoq_change"] / 1000
                change_df["direction"] = change_df["qoq_change"].apply(
                    lambda x: "Increase (Dilution)" if x > 0 else "Decrease (Accretion)"
                )
                color_scale = alt.Scale(
                    domain=["Increase (Dilution)", "Decrease (Accretion)"],
                    range=["#F44336", "#4CAF50"],
                )

                change_chart = (
                    alt.Chart(change_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "period_end:T",
                            title="Quarter End",
                            axis=alt.Axis(
                                format="%b %Y",
                                labelAngle=-45,
                                tickCount={"interval": "month", "step": 3},
                                grid=True,
                                gridOpacity=0.15,
                            ),
                        ),
                        y=alt.Y(
                            "qoq_change_k:Q",
                            title="QoQ Change in Shares (Thousands)",
                            axis=alt.Axis(format=",.0f"),
                        ),
                        color=alt.Color("direction:N", scale=color_scale, title="Direction"),
                        tooltip=[
                            alt.Tooltip("period_end:T", title="Period", format="%b %Y"),
                            alt.Tooltip("qoq_change:Q", title="Change", format="+,.0f"),
                            alt.Tooltip("qoq_change_pct:Q", title="Change %", format="+.2f"),
                        ],
                    )
                    .properties(height=300)
                )
                st.altair_chart(change_chart, use_container_width=True)

        st.divider()

        # --- Buyback Section ---
        st.subheader("Share Buyback Activity")

        if not buyback_df.empty:
            # Summary metrics
            bb_cols = st.columns(4)
            bb_cols[0].metric(
                "Total Shares Repurchased",
                fmt_shares(summary.get("total_shares_repurchased")),
            )
            bb_cols[1].metric(
                "Total Repurchase Value",
                fmt_currency(summary.get("total_repurchase_value")),
            )
            if summary.get("latest_authorization_usd"):
                bb_cols[2].metric(
                    "Latest Authorization",
                    fmt_currency(summary.get("latest_authorization_usd")),
                )
            if summary.get("buyback_remaining_usd") is not None:
                bb_cols[3].metric(
                    "Est. Remaining (V1)",
                    fmt_currency(summary.get("buyback_remaining_usd")),
                )

            # Buyback value per quarter
            if buyback_df["repurchase_value_usd"].sum() > 0:
                bb_value_chart = (
                    alt.Chart(buyback_df)
                    .mark_bar(color="#FF6F00")
                    .encode(
                        x=alt.X(
                            "period_end:T",
                            title="Quarter End",
                            axis=alt.Axis(
                                format="%b %Y",
                                labelAngle=-45,
                                tickCount={"interval": "month", "step": 3},
                                grid=True,
                                gridOpacity=0.15,
                            ),
                        ),
                        y=alt.Y(
                            "repurchase_value_usd:Q",
                            title="Repurchase Value ($)",
                            axis=alt.Axis(format="$,.0f"),
                        ),
                        tooltip=[
                            alt.Tooltip("period_end:T", title="Period", format="%b %Y"),
                            alt.Tooltip("repurchase_value_usd:Q", title="Value ($)", format="$,.0f"),
                            alt.Tooltip("shares_repurchased:Q", title="Shares", format=",.0f"),
                        ],
                    )
                    .properties(height=350)
                )
                st.altair_chart(bb_value_chart, use_container_width=True)

            # Shares repurchased per quarter
            if buyback_df["shares_repurchased"].sum() > 0:
                buyback_df["bb_shares_k"] = buyback_df["shares_repurchased"] / 1000

                bb_shares_chart = (
                    alt.Chart(buyback_df)
                    .mark_bar(color="#1565C0")
                    .encode(
                        x=alt.X(
                            "period_end:T",
                            title="Quarter End",
                            axis=alt.Axis(
                                format="%b %Y",
                                labelAngle=-45,
                                tickCount={"interval": "month", "step": 3},
                                grid=True,
                                gridOpacity=0.15,
                            ),
                        ),
                        y=alt.Y(
                            "bb_shares_k:Q",
                            title="Shares Repurchased (Thousands)",
                            axis=alt.Axis(format=",.0f"),
                        ),
                        tooltip=[
                            alt.Tooltip("period_end:T", title="Period", format="%b %Y"),
                            alt.Tooltip("shares_repurchased:Q", title="Shares", format=",.0f"),
                        ],
                    )
                    .properties(height=300)
                )
                st.altair_chart(bb_shares_chart, use_container_width=True)

            # Cumulative repurchase value over time
            if "cumulative_value" in buyback_df.columns and buyback_df["cumulative_value"].sum() > 0:
                # Overlay authorization line if available
                layers = []

                cum_chart = (
                    alt.Chart(buyback_df)
                    .mark_area(
                        line={"color": "#E65100"},
                        color=alt.Gradient(
                            gradient="linear",
                            stops=[
                                alt.GradientStop(color="#E65100", offset=1),
                                alt.GradientStop(color="rgba(230,81,0,0.1)", offset=0),
                            ],
                            x1=1, x2=1, y1=1, y2=0,
                        ),
                    )
                    .encode(
                        x=alt.X(
                            "period_end:T",
                            title="Quarter End",
                            axis=alt.Axis(
                                format="%b %Y",
                                labelAngle=-45,
                                tickCount={"interval": "month", "step": 3},
                                grid=True,
                                gridOpacity=0.15,
                            ),
                        ),
                        y=alt.Y(
                            "cumulative_value:Q",
                            title="Cumulative Value ($)",
                            axis=alt.Axis(format="$,.0f"),
                        ),
                        tooltip=[
                            alt.Tooltip("period_end:T", title="Period", format="%b %Y"),
                            alt.Tooltip("cumulative_value:Q", title="Cumulative ($)", format="$,.0f"),
                        ],
                    )
                )
                layers.append(cum_chart)

                # Authorization line
                auth_data = buyback_df.dropna(subset=["authorization_usd"])
                if not auth_data.empty:
                    auth_line = (
                        alt.Chart(auth_data)
                        .mark_line(
                            strokeDash=[5, 5],
                            color="#4CAF50",
                            strokeWidth=2,
                        )
                        .encode(
                            x=alt.X("period_end:T"),
                            y=alt.Y("authorization_usd:Q"),
                            tooltip=[
                                alt.Tooltip("period_end:T", title="Period"),
                                alt.Tooltip("authorization_usd:Q", title="Authorization ($)", format="$,.0f"),
                            ],
                        )
                    )
                    layers.append(auth_line)

                st.caption("Cumulative Buyback Spend vs Authorization")
                combined = alt.layer(*layers).properties(height=350)
                st.altair_chart(combined, use_container_width=True)
        else:
            st.info(
                f"No buyback activity found for {ticker_input}. "
                f"This company may not have an active buyback program, "
                f"or may use non-standard XBRL tags."
            )

        st.divider()

        # --- Raw data tables ---
        with st.expander("Raw Data Tables"):
            if not primary_df.empty:
                st.caption(share_type_label)
                display = primary_df[
                    ["period_end", "fiscal_year", "fiscal_period", "form_type",
                     "diluted_shares", "qoq_change", "qoq_change_pct"]
                ].copy()
                display.columns = [
                    "Period End", "FY", "FP", "Filing",
                    share_type_label, "QoQ Change", "QoQ %",
                ]
                st.dataframe(display, use_container_width=True, hide_index=True)

            if not buyback_df.empty:
                st.caption("Buyback Activity")
                bb_display_cols = [
                    "period_end", "fiscal_year", "fiscal_period",
                    "shares_repurchased", "repurchase_value_usd",
                    "cumulative_value", "authorization_usd", "estimated_remaining_usd",
                ]
                available = [c for c in bb_display_cols if c in buyback_df.columns]
                bb_display = buyback_df[available].copy()
                bb_display.columns = [
                    c.replace("_", " ").title() for c in available
                ]
                st.dataframe(bb_display, use_container_width=True, hide_index=True)


# ============================================================
# WATCHLIST OVERVIEW PAGE
# ============================================================

elif page == "Watchlist Overview":
    st.header("Watchlist Overview")
    st.caption(
        "Companies from your Insider Monitor watchlist and your local watchlist. "
        "Select any to view dilution data."
    )

    # Read from insider monitor watchlist
    watchlist = sf.get_insider_watchlist()
    dilution_wl = sf.get_dilution_watchlist()
    ingested = sf.get_all_ingested_tickers()
    ingested_map = {r["TICKER"]: r for r in ingested}

    # --- Local (Dilution Monitor) watchlist ---
    if dilution_wl:
        st.subheader("My Watchlist")
        dwl_df = pd.DataFrame(dilution_wl)
        dwl_df.columns = [c.lower() for c in dwl_df.columns]
        dwl_df["status"] = dwl_df["ticker"].apply(
            lambda t: ingested_map.get(t, {}).get("STATUS", "Not ingested")
        )
        dwl_df["last_updated"] = dwl_df["ticker"].apply(
            lambda t: str(ingested_map.get(t, {}).get("LAST_INGESTED_AT", "-"))[:19]
        )
        display_dwl = dwl_df[["ticker", "company_name", "status", "last_updated"]].copy()
        display_dwl.columns = ["Ticker", "Company", "Dilution Data Status", "Last Updated"]
        st.dataframe(display_dwl, use_container_width=True, hide_index=True)
        st.divider()

    # --- Insider Monitor watchlist ---
    if watchlist:
        st.subheader("Insider Monitor Watchlist")
        wl_df = pd.DataFrame(watchlist)
        wl_df.columns = [c.lower() for c in wl_df.columns]

        # Add ingestion status
        wl_df["status"] = wl_df["ticker"].apply(
            lambda t: ingested_map.get(t, {}).get("STATUS", "Not ingested")
        )
        wl_df["last_updated"] = wl_df["ticker"].apply(
            lambda t: str(ingested_map.get(t, {}).get("LAST_INGESTED_AT", "-"))[:19]
        )

        display = wl_df[["ticker", "company_name", "status", "last_updated"]].copy()
        display.columns = ["Ticker", "Company", "Dilution Data Status", "Last Updated"]
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.divider()

        # Quick ingest for un-ingested tickers
        not_ingested = [
            t for t in wl_df["ticker"].tolist()
            if t not in ingested_map
        ]
        if not_ingested:
            st.subheader("Ingest Watchlist Companies")
            st.caption(
                f"{len(not_ingested)} companies have not been ingested yet."
            )
            if st.button(f"Ingest All ({len(not_ingested)} companies)"):
                progress = st.progress(0)
                for i, ticker in enumerate(not_ingested):
                    with st.spinner(f"Ingesting {ticker}..."):
                        ingest_company(ticker)
                    progress.progress((i + 1) / len(not_ingested))
                st.rerun()
    if not watchlist and not dilution_wl:
        st.info(
            "No watchlist companies found. Use the sidebar to add tickers to your watchlist, "
            "or search for a company on the Search page."
        )

    # Also show any companies ingested directly (not in either watchlist)
    all_wl_tickers = (
        {w.get("TICKER", "") for w in watchlist}
        | {w.get("TICKER", "") for w in dilution_wl}
    )
    extra = [
        r for r in ingested
        if r["TICKER"] not in all_wl_tickers
    ]
    if extra:
        st.divider()
        st.subheader("Other Tracked Companies")
        extra_df = pd.DataFrame(extra)
        extra_df.columns = [c.lower() for c in extra_df.columns]
        display = extra_df[["ticker", "entity_name", "status", "last_ingested_at"]].copy()
        display.columns = ["Ticker", "Company", "Status", "Last Updated"]
        st.dataframe(display, use_container_width=True, hide_index=True)
