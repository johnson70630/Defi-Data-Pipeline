import os
from datetime import UTC, datetime

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from cryptography.hazmat.primitives import serialization

st.set_page_config(
    page_title="DeFi Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_KEY_PASSPHRASE")


def load_private_key() -> bytes:
    if not PRIVATE_KEY_PATH:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH is missing.")

    if not PRIVATE_KEY_PASSPHRASE:
        raise ValueError("SNOWFLAKE_KEY_PASSPHRASE is missing.")

    with open(PRIVATE_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=PRIVATE_KEY_PASSPHRASE.encode(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "JC01541.us-east-2.aws"),
        user=os.getenv("SNOWFLAKE_USER", "DBT_USER"),
        private_key=load_private_key(),
        role=os.getenv("SNOWFLAKE_ROLE", "DBT_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "DEFI_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "DEFI_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "STAGING"),
    )


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)


st.title("DeFi Analytics Dashboard")
st.caption("Uniswap swap analytics powered by Airflow, AWS S3, Snowflake, and dbt")

try:
    daily_df = query(
        """
        SELECT
            event_date,
            swap_count,
            active_senders,
            total_volume_usd,
            avg_swap_usd,
            max_swap_usd
        FROM daily_swap_volume
        ORDER BY event_date
        """
    )

    token_df = query(
        """
        SELECT
            token0_symbol,
            token1_symbol,
            swap_count,
            total_volume_usd,
            avg_swap_usd
        FROM token_volume
        ORDER BY total_volume_usd DESC
        LIMIT 15
        """
    )

    pool_df = query(
        """
        SELECT
            pool_id,
            token0_symbol,
            token1_symbol,
            swap_count,
            total_volume_usd,
            avg_swap_usd,
            max_swap_usd
        FROM pool_volume
        ORDER BY total_volume_usd DESC
        LIMIT 15
        """
    )

    raw_summary_df = query(
        """
        SELECT
            COUNT(*) AS raw_row_count,
            MAX(event_time) AS latest_event_time,
            MAX(event_date) AS latest_event_date
        FROM DEFI_DB.RAW.RAW_UNISWAP_SWAPS
        """
    )
except Exception as exc:
    st.error(f"Failed to load dashboard data: {exc}")
    st.stop()

if daily_df.empty:
    st.warning("No data found in the dbt mart tables yet.")
    st.stop()

# Snowflake returns unquoted column names in uppercase, so normalize them.
daily_df.columns = [col.lower() for col in daily_df.columns]
token_df.columns = [col.lower() for col in token_df.columns]
pool_df.columns = [col.lower() for col in pool_df.columns]
raw_summary_df.columns = [col.lower() for col in raw_summary_df.columns]

latest_event_time = raw_summary_df.loc[0, "latest_event_time"]
latest_event_date = raw_summary_df.loc[0, "latest_event_date"]
raw_row_count = raw_summary_df.loc[0, "raw_row_count"]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Volume USD", f"${daily_df['total_volume_usd'].sum():,.2f}")
col2.metric("Total Swaps", f"{daily_df['swap_count'].sum():,}")
col3.metric("Average Swap USD", f"${daily_df['avg_swap_usd'].mean():,.2f}")
col4.metric("Raw Rows", f"{raw_row_count:,}")

st.caption(
    f"Latest blockchain event date: {latest_event_date} | "
    f"Latest event time: {latest_event_time} | "
    f"Dashboard refreshed at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
)

st.divider()

st.subheader("Daily Swap Volume")
daily_volume_fig = px.line(
    daily_df,
    x="event_date",
    y="total_volume_usd",
    markers=True,
    labels={
        "event_date": "Event Date",
        "total_volume_usd": "Total Volume USD",
    },
    title="Daily Uniswap Swap Volume",
)
st.plotly_chart(daily_volume_fig, use_container_width=True)

left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Top Token Pairs by Volume")
    token_df["token_pair"] = token_df["token0_symbol"] + "-" + token_df["token1_symbol"]
    token_fig = px.bar(
        token_df,
        x="token_pair",
        y="total_volume_usd",
        hover_data=["swap_count", "avg_swap_usd"],
        labels={
            "token_pair": "Token Pair",
            "total_volume_usd": "Total Volume USD",
        },
        title="Top Token Pairs",
    )
    st.plotly_chart(token_fig, use_container_width=True)

with right_col:
    st.subheader("Top Pools by Volume")
    pool_display_df = pool_df.copy()
    pool_display_df["pool_label"] = (
        pool_display_df["token0_symbol"]
        + "-"
        + pool_display_df["token1_symbol"]
        + " | "
        + pool_display_df["pool_id"].str.slice(0, 8)
        + "..."
    )
    pool_fig = px.bar(
        pool_display_df,
        x="pool_label",
        y="total_volume_usd",
        hover_data=["pool_id", "swap_count", "avg_swap_usd", "max_swap_usd"],
        labels={
            "pool_label": "Pool",
            "total_volume_usd": "Total Volume USD",
        },
        title="Top Liquidity Pools",
    )
    st.plotly_chart(pool_fig, use_container_width=True)

st.subheader("Top Pool Details")
st.dataframe(
    pool_df[
        [
            "pool_id",
            "token0_symbol",
            "token1_symbol",
            "swap_count",
            "total_volume_usd",
            "avg_swap_usd",
            "max_swap_usd",
        ]
    ],
    use_container_width=True,
)