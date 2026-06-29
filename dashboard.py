import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import time

st.set_page_config(
    page_title="Ecommerce Clickstream Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 Real-Time Ecommerce Clickstream Dashboard")
st.markdown("Live data from Kafka → Spark → PostgreSQL")


def get_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="clickstream",
        user="sparkuser",
        password="sparkpass"
    )


def load_data():
    try:
        conn = get_connection()
        df = pd.read_sql(
            "SELECT * FROM clickstream_events ORDER BY event_time DESC",
            conn
        )
        conn.close()

        # Convert UTC → IST for all time-related display
        df["event_time"] = (
            pd.to_datetime(df["event_time"])
            .dt.tz_localize("UTC")
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )
        return df

    except Exception as e:
        st.error(f"DB Error: {e}")
        return pd.DataFrame()


while True:
    df = load_data()

    if df.empty:
        st.warning("⏳ Waiting for data... Make sure producer.py is running!")
        time.sleep(5)
        st.rerun()

    # ── KPI Cards ─────────────────────────────────────────────────
    st.subheader("📊 Live Event Counts")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events", len(df))
    col2.metric("🖱️ Clicks",    len(df[df["action"] == "click"]))
    col3.metric("💰 Purchases", len(df[df["action"] == "purchase"]))
    col4.metric("👁️ Views",     len(df[df["action"] == "view"]))

    st.divider()

    # ── Charts ────────────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🏆 Top Products by Action")
        product_action = df.groupby(["product", "action"]).size().reset_index(name="count")
        fig1 = px.bar(
            product_action,
            x="product",
            y="count",
            color="action",
            barmode="group",
            color_discrete_map={"click": "#636EFA", "purchase": "#00CC96", "view": "#EF553B"}
        )
        fig1.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig1, use_container_width=True)

    with col_right:
        st.subheader("📈 Actions Over Time (IST)")
        df["minute"] = df["event_time"].dt.floor("min")
        time_series = df.groupby(["minute", "action"]).size().reset_index(name="count")
        fig2 = px.line(
            time_series,
            x="minute",
            y="count",
            color="action",
            markers=True,
            color_discrete_map={"click": "#636EFA", "purchase": "#00CC96", "view": "#EF553B"}
        )
        fig2.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # ── Events Table ──────────────────────────────────────────────
    st.subheader("📋 Real-Time Events (Latest 50)")
    st.dataframe(
        df[["event_time", "user", "product", "action"]].head(50),
        use_container_width=True,
        hide_index=True
    )

    st.caption(f"🕐 Last refreshed (IST): {pd.Timestamp.now().strftime('%H:%M:%S')} — auto-refreshes every 5s")
    time.sleep(5)
    st.rerun()
