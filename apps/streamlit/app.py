"""Streamlit Customer 360 Dashboard — Main Entry Point."""

from __future__ import annotations

import streamlit as st
import plotly.express as px

from services.clickhouse_client import query_df
from services import queries
from components.kpi_cards import render_kpi_row

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Customer 360 Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS — clean light theme with accent colors
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* ---- Dark Theme ---- */

    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
    }
    .stApp {
        background-color: #0f1724;
    }

    /* Sidebar — dark navy with light text */
    section[data-testid="stSidebar"] {
        background-color: #0e1525 !important;
        border-right: 1px solid #1e293b;
    }
    section[data-testid="stSidebar"] * {
        color: #cbd5e1 !important;
    }
    section[data-testid="stSidebar"] .stRadio label:hover {
        color: #ffffff !important;
    }
    section[data-testid="stSidebar"] .stRadio label[data-checked="true"],
    section[data-testid="stSidebar"] [aria-checked="true"] {
        color: #818cf8 !important;
    }

    /* KPI metric cards — dark surface */
    div[data-testid="stMetric"] {
        background: #1a2332;
        border: 1px solid #2d3a4d;
        border-left: 4px solid #6366f1;
        border-radius: 8px;
        padding: 16px 20px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.25);
    }
    div[data-testid="stMetric"] label {
        color: #94a3b8 !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }

    /* Headings & text */
    .main h1, .main h2, .main h3, .main .stMarkdown h1,
    .main .stMarkdown h2, .main .stMarkdown h3 {
        color: #f1f5f9 !important;
    }
    .main p, .main span, .main label, .main .stCaption {
        color: #cbd5e1 !important;
    }

    /* Dividers */
    hr {
        border-color: #1e293b !important;
    }

    /* Dataframe / table */
    .stDataFrame {
        border: 1px solid #2d3a4d;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Plotly color palette & layout defaults
# ---------------------------------------------------------------------------

COLORS = {
    "primary": "#6366f1",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#f43f5e",
    "info": "#06b6d4",
    "purple": "#8b5cf6",
}

PALETTE = [COLORS["primary"], COLORS["success"], COLORS["warning"],
           COLORS["danger"], COLORS["info"], COLORS["purple"]]


def _plotly_layout(fig, height: int = 400):
    """Apply a consistent clean layout to a Plotly figure."""
    fig.update_layout(
        height=height,
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#cbd5e1"),
        xaxis=dict(gridcolor="#1e293b", color="#94a3b8"),
        yaxis=dict(gridcolor="#1e293b", color="#94a3b8"),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            font=dict(color="#cbd5e1"),
        ),
    )
    return fig


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("📊 Customer 360")
    st.caption("Data Platform E-commerce Dashboard")
    st.divider()
    page = st.radio(
        "Navigation",
        ["🏠 Overview", "👥 Customers", "📦 Orders & Products", "🖱️ Clickstream", "🎫 Support Tickets"],
        label_visibility="collapsed",
    )

# ---------------------------------------------------------------------------
# Helper formatters
# ---------------------------------------------------------------------------

def fmt_number(n) -> str:
    if n is None:
        return "0"
    n = float(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return f"{n:,.0f}"


def fmt_currency(n) -> str:
    if n is None:
        return "R$ 0"
    n = float(n)
    if n >= 1_000_000:
        return f"R$ {n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"R$ {n / 1_000:.1f}K"
    return f"R$ {n:,.2f}"


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Overview
# ═══════════════════════════════════════════════════════════════════════════

if page == "🏠 Overview":
    st.title("🏠 Tổng quan hệ thống")
    st.caption("Các chỉ số KPI chính của nền tảng E-commerce")

    # --- KPI Row ---
    customers = query_df(queries.TOTAL_CUSTOMERS).iloc[0, 0]
    orders = query_df(queries.TOTAL_ORDERS).iloc[0, 0]
    revenue = query_df(queries.TOTAL_REVENUE).iloc[0, 0]
    events = query_df(queries.TOTAL_EVENTS).iloc[0, 0]
    tickets = query_df(queries.TOTAL_TICKETS).iloc[0, 0]
    products = query_df(queries.TOTAL_PRODUCTS).iloc[0, 0]

    render_kpi_row([
        {"label": "👥 Tổng khách hàng", "value": fmt_number(customers)},
        {"label": "📦 Tổng đơn hàng", "value": fmt_number(orders)},
        {"label": "💰 Doanh thu (Completed)", "value": fmt_currency(revenue)},
        {"label": "🖱️ Clickstream Events", "value": fmt_number(events)},
        {"label": "🎫 Support Tickets", "value": fmt_number(tickets)},
        {"label": "🛍️ Sản phẩm", "value": fmt_number(products)},
    ])

    st.divider()

    # --- Revenue over time ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Doanh thu theo tháng")
        revenue_df = query_df(queries.REVENUE_BY_MONTH)
        if not revenue_df.empty:
            revenue_df["month"] = revenue_df["month"].astype(str).str[:7]
            fig = px.bar(
                revenue_df, x="month", y="revenue",
                labels={"month": "Tháng", "revenue": "Doanh thu (R$)"},
                color_discrete_sequence=[COLORS["primary"]],
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    with col2:
        st.subheader("📊 Đơn hàng theo trạng thái")
        status_df = query_df(queries.ORDERS_BY_STATUS)
        if not status_df.empty:
            fig = px.pie(
                status_df, names="order_status", values="cnt",
                color_discrete_sequence=PALETTE,
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="label+percent")
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    st.divider()

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("💳 Doanh thu theo phương thức thanh toán")
        payment_df = query_df(queries.REVENUE_BY_PAYMENT)
        if not payment_df.empty:
            fig = px.bar(
                payment_df, x="payment_method", y="revenue",
                labels={"payment_method": "Phương thức", "revenue": "Doanh thu (R$)"},
                color_discrete_sequence=[COLORS["warning"]],
            )
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    with col4:
        st.subheader("🏆 Phân bổ hạng thành viên")
        loyalty_df = query_df(queries.LOYALTY_DISTRIBUTION)
        if not loyalty_df.empty:
            tier_colors = {"Platinum": "#a78bfa", "Gold": "#fbbf24",
                           "Silver": "#94a3b8", "Bronze": "#d97706", "No Tier": "#e2e8f0"}
            fig = px.pie(
                loyalty_df, names="tier", values="cnt",
                color="tier", color_discrete_map=tier_colors,
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="label+percent")
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Customers
# ═══════════════════════════════════════════════════════════════════════════

elif page == "👥 Customers":
    st.title("👥 Customer 360")
    st.caption("Tra cứu và phân tích khách hàng")

    # --- Search ---
    search_term = st.text_input("🔍 Tìm theo tên hoặc User ID", placeholder="Nhập tên hoặc ID...")

    if search_term:
        search_pattern = f"%{search_term}%"
        df = query_df(queries.CUSTOMER_360_SEARCH, {"search": search_pattern})
        st.info(f"Tìm thấy {len(df)} kết quả cho '{search_term}'")
    else:
        total = query_df(queries.CUSTOMER_360_COUNT).iloc[0, 0]
        st.info(f"Tổng: {total:,} khách hàng (hiển thị top 100 theo LTV)")
        df = query_df(queries.CUSTOMER_360_TABLE.format(limit=100, offset=0))

    if not df.empty:
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "user_id": st.column_config.TextColumn("User ID", width="small"),
                "full_name": st.column_config.TextColumn("Họ tên", width="medium"),
                "customer_city": st.column_config.TextColumn("Thành phố"),
                "customer_state": st.column_config.TextColumn("Bang"),
                "loyalty_tier": st.column_config.TextColumn("Hạng TV"),
                "total_lifetime_value": st.column_config.NumberColumn("LTV (R$)", format="R$ %.2f"),
                "total_orders_completed": st.column_config.NumberColumn("Đơn hoàn thành"),
                "total_abandoned_carts": st.column_config.NumberColumn("Bỏ giỏ"),
                "total_cs_complaints": st.column_config.NumberColumn("Khiếu nại"),
                "last_active_date": st.column_config.DatetimeColumn("Hoạt động cuối"),
            },
        )

    st.divider()

    # --- Geographic distribution ---
    st.subheader("🗺️ Top 10 bang theo doanh thu")
    states_df = query_df(queries.TOP_STATES)
    if not states_df.empty:
        fig = px.bar(
            states_df, x="customer_state", y="revenue",
            labels={"customer_state": "Bang", "revenue": "Doanh thu (R$)"},
            color_discrete_sequence=[COLORS["primary"]],
            text_auto=".2s",
        )
        st.plotly_chart(_plotly_layout(fig), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Orders & Products
# ═══════════════════════════════════════════════════════════════════════════

elif page == "📦 Orders & Products":
    st.title("📦 Phân tích Đơn hàng & Sản phẩm")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Doanh thu theo tháng")
        revenue_df = query_df(queries.REVENUE_BY_MONTH)
        if not revenue_df.empty:
            revenue_df["month"] = revenue_df["month"].astype(str).str[:7]
            fig = px.line(
                revenue_df, x="month", y="revenue",
                labels={"month": "Tháng", "revenue": "Doanh thu (R$)"},
                markers=True,
                color_discrete_sequence=[COLORS["primary"]],
            )
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    with col2:
        st.subheader("📊 Số đơn theo tháng")
        if not revenue_df.empty:
            fig = px.bar(
                revenue_df, x="month", y="order_count",
                labels={"month": "Tháng", "order_count": "Số đơn"},
                color_discrete_sequence=[COLORS["success"]],
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    st.divider()

    st.subheader("🏆 Top 10 sản phẩm theo doanh thu")
    products_df = query_df(queries.TOP_PRODUCTS)
    if not products_df.empty:
        fig = px.bar(
            products_df, x="revenue", y="product_name",
            orientation="h",
            labels={"product_name": "", "revenue": "Doanh thu (R$)"},
            color_discrete_sequence=[COLORS["purple"]],
            text_auto=".2s",
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(_plotly_layout(fig, height=500), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Clickstream
# ═══════════════════════════════════════════════════════════════════════════

elif page == "🖱️ Clickstream":
    st.title("🖱️ Phân tích Clickstream")
    st.caption("Hành vi người dùng trên web/app")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Phân bổ loại sự kiện")
        events_df = query_df(queries.EVENTS_BY_TYPE)
        if not events_df.empty:
            fig = px.pie(
                events_df, names="event_type", values="cnt",
                color_discrete_sequence=PALETTE,
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="label+percent")
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    with col2:
        st.subheader("📱 Phân bổ thiết bị")
        device_df = query_df(queries.DEVICE_DISTRIBUTION)
        if not device_df.empty:
            fig = px.pie(
                device_df, names="device_os", values="cnt",
                color_discrete_sequence=[COLORS["info"], COLORS["warning"], COLORS["danger"]],
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="label+percent")
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    st.divider()

    st.subheader("📈 Xu hướng sự kiện theo ngày")
    daily_df = query_df(queries.EVENTS_BY_DAY)
    if not daily_df.empty:
        daily_df["day"] = daily_df["day"].astype(str)
        fig = px.line(
            daily_df, x="day", y="cnt", color="event_type",
            labels={"day": "Ngày", "cnt": "Số sự kiện", "event_type": "Loại"},
            color_discrete_sequence=PALETTE,
            markers=True,
        )
        st.plotly_chart(_plotly_layout(fig, height=450), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Support Tickets
# ═══════════════════════════════════════════════════════════════════════════

elif page == "🎫 Support Tickets":
    st.title("🎫 Phân tích Support Tickets")
    st.caption("Dữ liệu chăm sóc khách hàng")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Tickets theo phân loại")
        cat_df = query_df(queries.TICKETS_BY_CATEGORY)
        if not cat_df.empty:
            fig = px.bar(
                cat_df, x="category", y="cnt",
                labels={"category": "Phân loại", "cnt": "Số tickets"},
                color_discrete_sequence=[COLORS["danger"]],
                text_auto=True,
            )
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)

    with col2:
        st.subheader("📊 Tickets theo trạng thái")
        status_df = query_df(queries.TICKETS_BY_STATUS)
        if not status_df.empty:
            fig = px.pie(
                status_df, names="status", values="cnt",
                color_discrete_sequence=[COLORS["success"], COLORS["warning"]],
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="label+percent")
            st.plotly_chart(_plotly_layout(fig), use_container_width=True)
