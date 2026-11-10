"""Streamlit dashboard for Wikimedia's live RecentChange stream."""
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

load_dotenv(Path(__file__).with_name(".env"), override=True)

DB = Path(
    os.getenv(
        "WIKIMEDIA_DB",
        str(Path(__file__).with_name("wikimedia_live.db")),
    )
)
REFRESH_MS = 5_000
SPARK_REPORT = Path(
    os.getenv(
        "WIKIMEDIA_ANALYTICS_REPORT",
        str(Path(__file__).with_name("spark_data") / "wikimedia" / "latest_analytics.json"),
    )
)
SPARK_MASTER_UI = os.getenv("SPARK_MASTER_UI", "http://localhost:8080")


@st.cache_data(ttl=3)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    if not DB.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB, timeout=10) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "recent_changes" not in tables:
            return pd.DataFrame()
        return pd.read_sql_query(sql, connection, params=params)


def load_state() -> dict[str, str]:
    try:
        with sqlite3.connect(DB, timeout=10) as connection:
            return dict(
                connection.execute(
                    "SELECT key, value FROM consumer_state"
                ).fetchall()
            )
    except sqlite3.Error:
        return {}


def number(state: dict[str, str], key: str) -> int:
    try:
        return int(state.get(key, 0))
    except (TypeError, ValueError):
        return 0


def format_age(timestamp: str | None) -> tuple[str, float | None]:
    if not timestamp:
        return "No event received yet", None
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        age_seconds = max(
            0.0, (datetime.now(timezone.utc) - parsed).total_seconds()
        )
    except (TypeError, ValueError):
        return "Unknown", None
    if age_seconds < 60:
        return f"{int(age_seconds)} seconds ago", age_seconds
    return f"{int(age_seconds // 60)} minutes ago", age_seconds


@st.cache_data(ttl=3)
def load_spark_report(path: str) -> dict:
    try:
        with Path(path).open(encoding="utf-8") as report_file:
            report = json.load(report_file)
        return report if isinstance(report, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


@st.cache_data(ttl=10)
def get_spark_cluster_status(master_ui_url: str) -> dict:
    try:
        response = requests.get(f"{master_ui_url.rstrip('/')}/json/", timeout=1.5)
        response.raise_for_status()
        payload = response.json()
        workers = [
            worker
            for worker in payload.get("workers", [])
            if str(worker.get("state", "")).upper() == "ALIVE"
        ]
        return {
            "available": True,
            "workers": len(workers),
            "cores": sum(int(worker.get("cores", 0)) for worker in workers),
            "cores_used": sum(int(worker.get("coresused", 0)) for worker in workers),
            "apps": len(payload.get("activeapps", [])),
        }
    except (requests.RequestException, ValueError, TypeError):
        return {
            "available": False,
            "workers": 0,
            "cores": 0,
            "cores_used": 0,
            "apps": 0,
        }


def run() -> None:
    st.set_page_config(
        page_title="Wikimedia Live | Big Data Dashboard",
        page_icon="📰",
        layout="wide",
    )
    st_autorefresh(interval=REFRESH_MS, key="wikimedia-live-refresh")
    st.markdown(
        """
        <style>
        :root {
            color-scheme: light;
            --ink: #182b3d;
            --muted: #617488;
            --line: #e4ebf2;
            --panel: #ffffff;
            --canvas: #f3f6fa;
            --blue: #2864dc;
        }
        .stApp { background: var(--canvas); color: var(--ink); }
        [data-testid="stHeader"] { background: rgba(243, 246, 250, .88); }
        [data-testid="stSidebar"] {
            background: #edf2f7;
            border-right: 1px solid var(--line);
        }
        [data-testid="stSidebar"] h2 { font-size: 1.05rem; }
        [data-testid="stMainBlockContainer"] {
            max-width: 1540px;
            padding-top: 1.8rem;
            padding-bottom: 3rem;
        }
        .hero {
            position: relative;
            overflow: hidden;
            background:
                radial-gradient(circle at 90% 15%, rgba(119, 190, 255, .30), transparent 25%),
                linear-gradient(118deg, #10243b 0%, #173c65 55%, #2864a6 100%);
            color: #fff;
            padding: clamp(24px, 4vw, 42px);
            border: 1px solid rgba(255,255,255,.12);
            border-radius: 22px;
            margin-bottom: 18px;
            box-shadow: 0 16px 38px rgba(17, 44, 75, .14);
        }
        .hero-kicker {
            color: #a9d8ff;
            font-size: .73rem;
            font-weight: 750;
            letter-spacing: .16em;
            text-transform: uppercase;
            margin-bottom: 9px;
        }
        .hero h1 {
            color: #fff;
            font-size: clamp(1.8rem, 3vw, 2.65rem);
            letter-spacing: -.035em;
            line-height: 1.12;
            margin: 0;
        }
        .hero p { color: #d7e8f7; font-size: 1rem; margin: 10px 0 0; }
        .section-kicker {
            color: #657b91;
            font-size: .72rem;
            font-weight: 750;
            letter-spacing: .13em;
            text-transform: uppercase;
            margin: 1.4rem 0 .45rem;
        }
        div[data-testid="stMetric"] {
            min-height: 112px;
            padding: 17px 19px;
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 15px;
            box-shadow: 0 3px 12px rgba(27, 55, 80, .035);
        }
        [data-testid="stMetricLabel"] p {
            color: var(--muted);
            font-size: .78rem;
            font-weight: 650;
        }
        [data-testid="stMetricValue"] {
            color: var(--ink);
            font-size: clamp(1.25rem, 2vw, 1.8rem);
            font-weight: 720;
            letter-spacing: -.03em;
        }
        div[data-testid="stTabs"] button[role="tab"] {
            color: var(--muted);
            font-weight: 650;
        }
        div[data-testid="stTabs"] button[aria-selected="true"] { color: var(--blue); }
        div[data-testid="stTabs"] [data-baseweb="tab-highlight"] { background: var(--blue); }
        div[data-testid="stDataFrame"] {
            border: 1px solid var(--line);
            border-radius: 14px;
            overflow: hidden;
        }
        div[data-testid="stAlert"] { border-radius: 12px; }
        [data-testid="stCaptionContainer"] { color: var(--muted); }
        @media (max-width: 760px) {
            [data-testid="stMainBlockContainer"] { padding: 1rem 1rem 2rem; }
            .hero { border-radius: 16px; }
            div[data-testid="stMetric"] { min-height: 94px; padding: 13px; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="hero">
          <div class="hero-kicker">Real-time data observability</div>
          <h1>Wikimedia Live Changes</h1>
          <p>Follow global edits as they flow through EventStreams, Kafka, and distributed Spark analytics.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    state = load_state()
    stream_age, stream_seconds = format_age(state.get("stream_status_at"))
    last_event_age, event_seconds = format_age(state.get("last_event_received_at"))
    stream_fresh = (
        state.get("stream_status") == "connected"
        and stream_seconds is not None
        and stream_seconds < 90
    )
    feed_status = "CONNECTED" if stream_fresh else (
        "RECONNECTING" if state.get("stream_status") == "reconnecting" else "WAITING"
    )
    total_events = number(state, "recent_changes_consumed")
    invalid_count = number(state, "invalid_events")

    if not stream_fresh:
        status_color = "#b56b16"
        st.markdown(
            f'<div class="section-kicker">Pipeline health &nbsp;·&nbsp; '
            f'<span style="color:{status_color}">{feed_status}</span> '
            f'&nbsp;·&nbsp; Signal {stream_age} &nbsp;·&nbsp; Updated every 5 sec</div>',
            unsafe_allow_html=True,
        )
    if state.get("stream_status") == "reconnecting":
        st.warning(
            "The producer is reconnecting to Wikimedia. "
            f"{state.get('stream_status_detail', '')}"
        )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Source stream", feed_status, help="Wikimedia SSE connection state")
    metric_cols[1].metric("Changes processed", f"{total_events:,}", help="Recent changes written to SQLite")
    metric_cols[2].metric("Latest change", last_event_age, help="Age of the most recently stored event")
    metric_cols[3].metric("Invalid messages", f"{invalid_count:,}", help="Malformed Kafka events recorded")
    if not stream_fresh and state.get("stream_status") != "reconnecting":
        st.caption(
            "Stream is waiting for a producer heartbeat. Start `live_producer.py` and `stream_consumer.py` to resume ingestion."
        )
    elif event_seconds is None or event_seconds > 120:
        st.caption(
            "No recent edit event is stored; the source may be quiet or the consumer may need attention."
        )

    wiki_rows = query("SELECT DISTINCT wiki FROM recent_changes ORDER BY wiki")
    wiki_options = ["All wikis"]
    if not wiki_rows.empty:
        wiki_options.extend(wiki_rows["wiki"].dropna().astype(str).tolist())
    with st.sidebar:
        st.markdown("## Dashboard controls")
        st.caption("Filter the feed and all analytics below.")
        lookback_label = st.selectbox(
            "Time window",
            ["Last hour", "Last 6 hours", "Last 24 hours", "Last 7 days", "All retained"],
            index=2,
        )
        selected_wiki = st.selectbox("Wiki", wiki_options)
        lookback_sql = {
            "Last hour": "-1 hour",
            "Last 6 hours": "-6 hours",
            "Last 24 hours": "-24 hours",
            "Last 7 days": "-7 days",
            "All retained": None,
        }[lookback_label]
        st.caption(
            "Source: Wikimedia EventStreams recentchange (Server-Sent Events)."
        )
        st.caption(
            f"Kafka consumer last committed: {state.get('last_consumed_at', 'waiting')}"
        )
        st.caption(
            "Usernames and edit comments are intentionally not stored by this project."
        )

    where = (
        "WHERE (? IS NULL OR strftime('%s', occurred_at) >= "
        "strftime('%s', 'now', ?)) AND (? = 'All wikis' OR wiki = ?)"
    )
    filter_params = (
        lookback_sql,
        lookback_sql,
        selected_wiki,
        selected_wiki,
    )
    arrival_where = (
        "WHERE (? IS NULL OR strftime('%s', received_at) >= "
        "strftime('%s', 'now', ?)) AND (? = 'All wikis' OR wiki = ?)"
    )
    recent = query(
        f"""
        SELECT occurred_at, wiki, change_type, namespace, title, page_url,
               length_delta, bot, minor
        FROM recent_changes
        {where}
        ORDER BY occurred_at DESC
        LIMIT 250
        """,
        filter_params,
    )

    data_tab, feed_tab, analytics_tab, spark_tab = st.tabs(
        ["Data & processing", "Live edit feed", "8 Vs analytics", "Spark cluster"]
    )

    with data_tab:
        st.subheader("What data does this project process?")
        st.write(
            "The source is Wikimedia's global **RecentChange event stream**. "
            "Each record describes a page change; this project does not download "
            "full article text. The table below describes the fields retained for "
            "analysis. Usernames and edit comments are excluded."
        )

        st.markdown("#### Data variety · different kinds of event information")
        data_groups = pd.DataFrame(
            [
                {
                    "Data group": "Event identity & time",
                    "Fields": "event_id, occurred_at, received_at, stream",
                    "What it tells us": "Which change happened and when it was observed",
                },
                {
                    "Data group": "Wiki & page context",
                    "Fields": "wiki, server_name, title, page_url, namespace",
                    "What it tells us": "Which Wikimedia project and page area changed",
                },
                {
                    "Data group": "Change classification",
                    "Fields": "change_type, bot, minor",
                    "What it tells us": "Whether it was an edit, new page, log, categorization, and related flags",
                },
                {
                    "Data group": "Revision measurements",
                    "Fields": "revision_old/new, length_old/new, length_delta",
                    "What it tells us": "Revision sequence and page-size change; not article content",
                },
                {
                    "Data group": "Data pipeline measurements",
                    "Fields": "message_bytes, stream status, invalid/duplicate counters",
                    "What it tells us": "Payload volume, connection health, and data quality",
                },
            ]
        )
        st.dataframe(data_groups, hide_index=True, width="stretch")

        types = query(
            f"""
            SELECT change_type AS "Change type", COUNT(*) AS "Events"
            FROM recent_changes
            {where}
            GROUP BY change_type
            ORDER BY COUNT(*) DESC
            """,
            filter_params,
        )
        namespaces = query(
            f"""
            SELECT namespace AS id,
                   CASE namespace
                     WHEN 0 THEN 'Article'
                     WHEN 1 THEN 'Talk'
                     WHEN 2 THEN 'User'
                     WHEN 3 THEN 'User talk'
                     WHEN 4 THEN 'Project'
                     WHEN 5 THEN 'Project talk'
                     WHEN 6 THEN 'File'
                     WHEN 7 THEN 'File talk'
                     WHEN 10 THEN 'Template'
                     WHEN 11 THEN 'Template talk'
                     WHEN 14 THEN 'Category'
                     WHEN 15 THEN 'Category talk'
                     ELSE 'Other / project-specific'
                   END AS "Page area",
                   COUNT(*) AS "Events"
            FROM recent_changes
            {where}
            GROUP BY namespace
            ORDER BY COUNT(*) DESC
            """,
            filter_params,
        )
        type_col, namespace_col = st.columns(2)
        with type_col:
            st.markdown("##### Change types in this view")
            if types.empty:
                st.caption("No events match the current filters.")
            else:
                st.dataframe(types, hide_index=True, width="stretch")
        with namespace_col:
            st.markdown("##### Page namespaces in this view")
            st.caption(
                "Namespace IDs classify pages by purpose. Some IDs are project-specific."
            )
            if namespaces.empty:
                st.caption("No events match the current filters.")
            else:
                st.dataframe(namespaces, hide_index=True, width="stretch")

        st.markdown("#### How data flows through the project")
        stage_cols = st.columns(4)
        stages = [
            (
                "1 · COLLECT",
                "Wikimedia EventStreams sends continuous Server-Sent Events. "
                "`live_producer.py` parses and normalizes each event, omitting usernames and comments.",
            ),
            (
                "2 · TRANSPORT",
                "The producer publishes JSON to Kafka topic "
                "`wikimedia.recentchange.v1`, keyed by event ID for replay-safe processing.",
            ),
            (
                "3 · PROCESS",
                "`stream_consumer.py` validates events, removes duplicate IDs, and stores "
                "the event history in SQLite. Spark Structured Streaming independently "
                "aggregates Kafka micro-batches.",
            ),
            (
                "4 · ANALYZE",
                "The dashboard queries SQLite for the feed and eight-V metrics. "
                "Spark writes separate batch reports. The dashboard refreshes every five seconds.",
            ),
        ]
        for column, (heading, description) in zip(stage_cols, stages):
            with column:
                st.markdown(f"**{heading}**")
                st.write(description)

        st.info(
            "**What Variety means here:** the stream contains different change "
            "classifications, Wikimedia projects, page namespaces, and structured "
            "revision/quality fields. It is not a dataset of full article text, "
            "images, editor profiles, or edit comments."
        )

    with feed_tab:
        st.subheader("Latest Wikimedia changes")
        if recent.empty:
            st.info(
                "No edit events are stored yet. Start the producer and consumer, then wait for stream events."
            )
        else:
            display = recent.rename(
                columns={
                    "occurred_at": "Event time (UTC)",
                    "wiki": "Wiki",
                    "change_type": "Change",
                    "namespace": "Namespace",
                    "title": "Page",
                    "page_url": "Page link",
                    "length_delta": "Length change",
                    "bot": "Bot",
                    "minor": "Minor",
                }
            )
            st.dataframe(
                display,
                hide_index=True,
                width="stretch",
                column_config={
                    "Page link": st.column_config.LinkColumn(
                        "Page link", display_text="Open page"
                    ),
                    "Event time (UTC)": st.column_config.DatetimeColumn(
                        "Event time (UTC)", format="YYYY-MM-DD HH:mm:ss"
                    ),
                    "Length change": st.column_config.NumberColumn(
                        "Length change", help="Change in page length in bytes"
                    ),
                },
            )

    with analytics_tab:
        st.subheader("Big-data profile · eight Vs")
        st.caption(
            f"Filtered to **{selected_wiki}** · **{lookback_label.lower()}**. "
            "Event time is UTC; history is retained for up to seven days."
        )
        quality = query(
            f"""
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(message_bytes), 0) AS bytes,
                   COUNT(DISTINCT wiki) AS wiki_count,
                   COUNT(DISTINCT change_type) AS type_count,
                   COUNT(DISTINCT namespace) AS namespace_count,
                   SUM(CASE WHEN wiki <> '' THEN 1 ELSE 0 END) AS valid_wiki,
                   SUM(CASE WHEN title <> '' THEN 1 ELSE 0 END) AS valid_title,
                   SUM(CASE WHEN occurred_at <> '' THEN 1 ELSE 0 END) AS valid_time,
                   SUM(CASE WHEN event_id <> '' THEN 1 ELSE 0 END) AS valid_id,
                   SUM(CASE WHEN page_url LIKE 'http%' THEN 1 ELSE 0 END) AS valid_url,
                   SUM(CASE WHEN message_bytes > 0 THEN 1 ELSE 0 END) AS valid_payload,
                   SUM(CASE WHEN length_delta IS NOT NULL THEN 1 ELSE 0 END) AS with_length_delta,
                   COALESCE(SUM(CASE WHEN length_delta > 0 THEN length_delta ELSE 0 END), 0) AS bytes_added,
                   COALESCE(SUM(CASE WHEN length_delta < 0 THEN -length_delta ELSE 0 END), 0) AS bytes_removed,
                   COALESCE(SUM(length_delta), 0) AS net_length_delta,
                   COALESCE(SUM(bot), 0) AS bot_events,
                   COALESCE(SUM(minor), 0) AS minor_events,
                   MIN(occurred_at) AS oldest_event,
                   MAX(occurred_at) AS newest_event
            FROM recent_changes
            {where}
            """,
            filter_params,
        )
        stats = quality.iloc[0].to_dict() if not quality.empty else {}
        filtered_total = int(stats.get("total") or 0)
        payload_bytes = int(stats.get("bytes") or 0)
        complete_fields = sum(
            int(stats.get(key) or 0)
            for key in ("valid_wiki", "valid_title", "valid_time", "valid_id")
        )
        completeness = (
            100 * complete_fields / (filtered_total * 4) if filtered_total else 0.0
        )

        st.markdown("#### Volume · how much data")
        cols = st.columns(4)
        cols[0].metric("Events in view", f"{filtered_total:,}")
        cols[1].metric("Retained payload", f"{payload_bytes / (1024 * 1024):.2f} MB")
        cols[2].metric("Events consumed (all time)", f"{total_events:,}")
        cols[3].metric("Invalid messages (all time)", f"{invalid_count:,}")

        st.markdown("#### Velocity · how fast it arrives")
        last_five = query(
            f"""
            SELECT COUNT(*) AS count
            FROM recent_changes
            {arrival_where}
              AND strftime('%s', received_at) >= strftime('%s', 'now', '-5 minutes')
            """,
            filter_params,
        )
        five_minute_count = int(last_five.iloc[0]["count"]) if not last_five.empty else 0
        cols = st.columns(3)
        cols[0].metric("Events / minute (last 5m)", f"{five_minute_count / 5:.1f}")
        cols[1].metric("Last edit received", last_event_age)
        cols[2].metric("SSE connection", feed_status)
        st.caption(
            "Arrival rate uses the time each event reached this pipeline, so delayed source timestamps do not flatten the live trend."
        )
        activity = query(
            f"""
            SELECT strftime('%Y-%m-%dT%H:%M:00Z', received_at) AS minute,
                   change_type, COUNT(*) AS events
            FROM recent_changes
            {arrival_where}
            GROUP BY minute, change_type
            ORDER BY minute
            """,
            filter_params,
        )
        if not activity.empty:
            activity["minute"] = pd.to_datetime(
                activity["minute"], utc=True, errors="coerce"
            )
            activity = activity.dropna(subset=["minute"])
            chart = (
                alt.Chart(activity)
                .mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
                .encode(
                    x=alt.X("minute:T", title="Arrival time (UTC)", axis=alt.Axis(format="%H:%M")),
                    y=alt.Y("events:Q", title="Events received / minute", stack="zero"),
                    color=alt.Color("change_type:N", title="Change type"),
                    tooltip=[
                        alt.Tooltip("minute:T", title="Minute", format="%Y-%m-%d %H:%M"),
                        "change_type:N",
                        alt.Tooltip("events:Q", format=","),
                    ],
                )
                .properties(height=300)
                .interactive()
            )
            st.altair_chart(chart, width="stretch")
        else:
            st.info("No events were received in this time window yet.")

        st.markdown("#### Variety · what kinds of data")
        cols = st.columns(3)
        cols[0].metric("Wikis", f"{int(stats.get('wiki_count') or 0):,}")
        cols[1].metric("Change types", f"{int(stats.get('type_count') or 0):,}")
        cols[2].metric("Namespaces", f"{int(stats.get('namespace_count') or 0):,}")
        by_type = query(
            f"SELECT change_type, COUNT(*) AS events FROM recent_changes {where} "
            "GROUP BY change_type ORDER BY events DESC LIMIT 12",
            filter_params,
        )
        by_wiki = query(
            f"SELECT wiki, COUNT(*) AS events FROM recent_changes {where} "
            "GROUP BY wiki ORDER BY events DESC LIMIT 12",
            filter_params,
        )
        by_namespace = query(
            f"SELECT COALESCE(CAST(namespace AS TEXT), 'unknown') AS namespace, "
            f"COUNT(*) AS events FROM recent_changes {where} "
            "GROUP BY namespace ORDER BY events DESC LIMIT 12",
            filter_params,
        )
        left, middle, right = st.columns(3)
        if not by_type.empty:
            left.altair_chart(
                alt.Chart(by_type)
                .mark_bar(cornerRadiusEnd=5)
                .encode(
                    x=alt.X("events:Q", title="Events"),
                    y=alt.Y("change_type:N", title="Change type", sort="-x"),
                    color=alt.Color("change_type:N", legend=None),
                    tooltip=["change_type:N", alt.Tooltip("events:Q", format=",")],
                )
                .properties(height=260),
                width="stretch",
            )
        if not by_wiki.empty:
            middle.altair_chart(
                alt.Chart(by_wiki)
                .mark_bar(cornerRadiusEnd=5, color="#d47a35")
                .encode(
                    x=alt.X("events:Q", title="Events"),
                    y=alt.Y("wiki:N", title="Top wikis", sort="-x"),
                    tooltip=["wiki:N", alt.Tooltip("events:Q", format=",")],
                )
                .properties(height=260),
                width="stretch",
            )
        if not by_namespace.empty:
            right.altair_chart(
                alt.Chart(by_namespace)
                .mark_bar(cornerRadiusEnd=5, color="#2f8f6b")
                .encode(
                    x=alt.X("events:Q", title="Events"),
                    y=alt.Y("namespace:N", title="Namespace", sort="-x"),
                    tooltip=["namespace:N", alt.Tooltip("events:Q", format=",")],
                )
                .properties(height=260),
                width="stretch",
            )

        st.markdown("#### Veracity · can the feed be trusted?")
        cols = st.columns(3)
        cols[0].metric("Core field completeness", f"{completeness:.1f}%")
        cols[1].metric("Duplicate events ignored", f"{number(state, 'duplicate_events'):,}")
        cols[2].metric("Invalid events recorded", f"{invalid_count:,}")
        st.caption(
            "The consumer deduplicates event IDs and records invalid Kafka messages separately. "
            "Counters are cumulative for this local consumer."
        )

        st.markdown("#### Value · what changed?")
        cols = st.columns(4)
        cols[0].metric("Page bytes added", f"{int(stats.get('bytes_added') or 0):,}")
        cols[1].metric("Page bytes removed", f"{int(stats.get('bytes_removed') or 0):,}")
        cols[2].metric("Net page-length change", f"{int(stats.get('net_length_delta') or 0):+,}")
        delta_count = int(stats.get("with_length_delta") or 0)
        cols[3].metric(
            "Events with size data",
            f"{100 * delta_count / filtered_total:.1f}%" if filtered_total else "0.0%",
        )
        impact = query(
            f"""
            SELECT CASE WHEN length_delta > 0 THEN 'Bytes added'
                        WHEN length_delta < 0 THEN 'Bytes removed'
                        WHEN length_delta = 0 THEN 'No size change'
                        ELSE 'Not reported' END AS impact,
                   COUNT(*) AS events
            FROM recent_changes
            {where}
            GROUP BY impact
            ORDER BY events DESC
            """,
            filter_params,
        )
        if not impact.empty:
            st.altair_chart(
                alt.Chart(impact)
                .mark_arc(innerRadius=65, outerRadius=115)
                .encode(
                    theta=alt.Theta("events:Q"),
                    color=alt.Color("impact:N", title="Page-length impact"),
                    tooltip=["impact:N", alt.Tooltip("events:Q", format=",")],
                )
                .properties(height=300),
                width="stretch",
            )

        st.markdown("#### Variability · when does activity change?")
        interval_counts = query(
            f"""
            SELECT strftime('%Y-%m-%dT%H:', received_at) ||
                   printf('%02d', CAST(strftime('%M', received_at) AS INTEGER) / 5 * 5) ||
                   ':00Z' AS interval,
                   COUNT(*) AS events
            FROM recent_changes
            {arrival_where}
            GROUP BY interval
            ORDER BY interval
            """,
            filter_params,
        )
        if not interval_counts.empty:
            interval_counts["interval"] = pd.to_datetime(
                interval_counts["interval"], utc=True, errors="coerce"
            )
            interval_counts = interval_counts.dropna(subset=["interval"])
            intervals = (
                interval_counts.set_index("interval")
                .resample("5min")
                .sum()
                .reset_index()
            )
            mean_interval = float(intervals["events"].mean())
            std_interval = float(intervals["events"].std(ddof=0))
            peak = int(intervals["events"].max())
            cols = st.columns(3)
            cols[0].metric("Peak 5 minutes", f"{peak:,} events")
            cols[1].metric("5-minute average", f"{mean_interval:.1f}")
            cols[2].metric("5-minute variation", f"{std_interval:.1f} σ")
            st.caption(
                "Counts are grouped by pipeline arrival time; empty intervals are shown as zero."
            )
            st.altair_chart(
                alt.Chart(intervals)
                .mark_bar(color="#7759a8", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("interval:T", title="5-minute arrival interval (UTC)"),
                    y=alt.Y("events:Q", title="Events", scale=alt.Scale(zero=True)),
                    tooltip=[alt.Tooltip("interval:T", format="%Y-%m-%d %H:%M"), alt.Tooltip("events:Q", format=",")],
                )
                .properties(height=240)
                .interactive(),
                width="stretch",
            )
        else:
            st.info("No events were received in this time window yet.")

        st.markdown("#### Validity · does data match expected fields?")
        if filtered_total:
            checks = pd.DataFrame(
                [
                    {"Check": "Event ID", "Valid %": 100 * int(stats.get("valid_id") or 0) / filtered_total},
                    {"Check": "Wiki", "Valid %": 100 * int(stats.get("valid_wiki") or 0) / filtered_total},
                    {"Check": "Page title", "Valid %": 100 * int(stats.get("valid_title") or 0) / filtered_total},
                    {"Check": "Timestamp", "Valid %": 100 * int(stats.get("valid_time") or 0) / filtered_total},
                    {"Check": "Page URL", "Valid %": 100 * int(stats.get("valid_url") or 0) / filtered_total},
                    {"Check": "Payload bytes", "Valid %": 100 * int(stats.get("valid_payload") or 0) / filtered_total},
                ]
            )
            st.altair_chart(
                alt.Chart(checks)
                .mark_bar(cornerRadiusEnd=4)
                .encode(
                    x=alt.X("Valid %:Q", title="Records passing check (%)", scale=alt.Scale(domain=[0, 100])),
                    y=alt.Y("Check:N", title=None, sort="-x"),
                    color=alt.condition(alt.datum["Valid %"] < 100, alt.value("#d47a35"), alt.value("#2f8f6b")),
                    tooltip=["Check:N", alt.Tooltip("Valid %:Q", format=".1f")],
                )
                .properties(height=220),
                width="stretch",
            )
        else:
            st.info("No records are available to validate in this time window.")

        st.markdown("#### Volatility · freshness and retention")
        oldest_age, _ = format_age(stats.get("oldest_event"))
        newest_age, _ = format_age(stats.get("newest_event"))
        cols = st.columns(3)
        cols[0].metric("Oldest event in view", oldest_age)
        cols[1].metric("Newest event in view", newest_age)
        cols[2].metric("Retention policy", "7 days")
        daily = query(
            f"""
            SELECT strftime('%Y-%m-%d', occurred_at) AS day, COUNT(*) AS events
            FROM recent_changes
            {where}
            GROUP BY day
            ORDER BY day
            """,
            filter_params,
        )
        if not daily.empty:
            st.altair_chart(
                alt.Chart(daily)
                .mark_bar(color="#2f8f6b", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("day:T", title="Retained event day"),
                    y=alt.Y("events:Q", title="Events", scale=alt.Scale(zero=True)),
                    tooltip=["day:T", alt.Tooltip("events:Q", format=",")],
                )
                .properties(height=220),
                width="stretch",
            )

    with spark_tab:
        st.subheader("Distributed Spark analytics")
        cluster = get_spark_cluster_status(SPARK_MASTER_UI)
        cols = st.columns(4)
        cols[0].metric("Spark cluster", "Online" if cluster["available"] else "Offline")
        cols[1].metric("Alive workers", f"{cluster['workers']:,}")
        cols[2].metric(
            "Worker cores in use", f"{cluster['cores_used']:,} / {cluster['cores']:,}"
        )
        cols[3].metric("Active Spark apps", f"{cluster['apps']:,}")
        st.caption(f"Spark master UI: {SPARK_MASTER_UI}")

        report = load_spark_report(str(SPARK_REPORT))
        if not report:
            st.info(
                "Waiting for the Spark job's first analytics report. "
                "Check `docker compose logs -f spark-analytics`."
            )
        else:
            cols = st.columns(4)
            cols[0].metric("Kafka records in batch", f"{int(report.get('batch_kafka_records', 0)):,}")
            cols[1].metric("Recent changes analyzed", f"{int(report.get('batch_recent_changes', 0)):,}")
            cols[2].metric("Total records analyzed", f"{int(report.get('total_kafka_records', 0)):,}")
            cols[3].metric("Batch changes / sec", f"{float(report.get('recent_changes_per_second', 0)):.1f}")
            st.caption(
                f"Spark {report.get('spark_version', '?')} · "
                f"batch {report.get('batch_id', '?')} · "
                f"last output {report.get('processed_at', 'unknown')} · "
                f"workers processing data: "
                f"{', '.join(report.get('worker_hosts_used', [])) or 'pending'}"
            )
            event_counts = report.get("change_type_counts", {})
            if event_counts:
                frame = pd.DataFrame(
                    [
                        {"Change type": name, "Events": int(count)}
                        for name, count in event_counts.items()
                    ]
                )
                chart = (
                    alt.Chart(frame)
                    .mark_bar(cornerRadiusEnd=4, color="#386bb2")
                    .encode(
                        x=alt.X("Events:Q", title="Events"),
                        y=alt.Y("Change type:N", title=None, sort="-x"),
                        tooltip=["Change type:N", alt.Tooltip("Events:Q", format=",")],
                    )
                    .properties(height=250)
                )
                st.altair_chart(chart, width="stretch")
            st.json(report.get("data_quality", {}))


if __name__ == "__main__":
    run()
