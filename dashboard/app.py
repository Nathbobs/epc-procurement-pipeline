import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from dotenv import load_dotenv
from tracker import log_visitor

# Config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

log_visitor()
if st.session_state.get("_tracker_error"):
    st.error(f"Tracker error: {st.session_state._tracker_error}")
if st.session_state.get("_tracker_debug"):
    st.warning(f"Tracker debug: {st.session_state._tracker_debug}")

st.set_page_config(
    page_title="EPC Procurement Intelligence",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Foundry-inspired dark theme
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0b0e1a; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #111827;
        border-right: 1px solid #1e2d45;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background-color: #111827;
        border: 1px solid #1e2d45;
        border-radius: 8px;
        padding: 16px;
    }
    [data-testid="stMetricLabel"] { color: #64748b !important; font-size: 12px !important; }
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 28px !important; }
    [data-testid="stMetricDelta"] { color: #00d4ff !important; }

    /* Headers */
    h1, h2, h3 { color: #ffffff !important; }
    p, li { color: #cbd5e1 !important; }

    /* Divider */
    hr { border-color: #1e2d45; }

    /* Dataframe */
    .stDataFrame { border: 1px solid #1e2d45; border-radius: 8px; }

    /* Page title accent */
    .page-title {
        font-size: 22px;
        font-weight: 600;
        color: #ffffff;
        border-left: 3px solid #00d4ff;
        padding-left: 12px;
        margin-bottom: 24px;
    }

    /* Risk badge */
    .badge-high {
        background-color: #ff4444;
        color: white;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    .badge-ok {
        background-color: #10b981;
        color: white;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }

    /* Sidebar label */
    .sidebar-label {
        font-size: 10px;
        color: #00d4ff;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin-bottom: 4px;
    }

    /* Sidebar avatar block */
    .sb-avatar {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 4px 0 16px 0;
    }
    .sb-avatar-icon {
        width: 40px; height: 40px;
        background: linear-gradient(135deg, #00d4ff 0%, #0077ff 100%);
        border-radius: 10px;
        display: flex; align-items: center; justify-content: center;
        font-size: 20px;
        flex-shrink: 0;
    }
    .sb-avatar-name {
        font-size: 14px;
        font-weight: 700;
        color: #ffffff;
        line-height: 1.2;
    }
    .sb-avatar-sub {
        font-size: 11px;
        color: #64748b;
    }

    /* Sidebar section labels */
    .sb-section-label {
        font-size: 10px;
        color: #475569;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        padding: 12px 0 6px 0;
    }

    /* Sidebar static nav items (non-button) */
    .sb-nav-item {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 9px 12px;
        border-radius: 8px;
        font-size: 13px;
        color: #94a3b8;
        margin-bottom: 2px;
    }
    .sb-nav-icon { font-size: 15px; width: 20px; text-align: center; }

    /* Sidebar divider */
    .sb-divider { border-top: 1px solid #1e2d45; margin: 10px 0; }

    /* Sidebar footer */
    .sb-footer {
        font-size: 11px;
        color: #475569;
        padding-top: 8px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    /* Sidebar nav buttons — left-aligned, teal active overlay */
    [data-testid="stSidebar"] .stButton > button {
        background: transparent !important;
        border: none !important;
        color: #94a3b8 !important;
        font-size: 13px !important;
        display: flex !important;
        align-items: center !important;
        text-align: left !important;
        justify-content: flex-start !important;
        padding: 9px 14px !important;
        border-radius: 8px !important;
        width: 100% !important;
        transition: background 0.15s, color 0.15s !important;
    }
    [data-testid="stSidebar"] .stButton > button > div {
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] .stButton > button p {
        text-align: left !important;
        margin: 0 !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: rgba(0, 212, 255, 0.08) !important;
        color: #ffffff !important;
    }

    /* Active nav button — solid teal-tinted overlay like reference design */
    [data-testid="stSidebar"] .stButton > button[data-active="true"],
    [data-testid="stSidebar"] .stButton > button.active {
        background: rgba(0, 212, 255, 0.15) !important;
        color: #00d4ff !important;
        font-weight: 600 !important;
    }
</style>
""", unsafe_allow_html=True)

# Plotly theme
PLOTLY_LAYOUT = dict(
    paper_bgcolor="#111827",
    plot_bgcolor="#111827",
    font=dict(color="#cbd5e1", family="monospace"),
    xaxis=dict(gridcolor="#1e2d45", zerolinecolor="#1e2d45"),
    yaxis=dict(gridcolor="#1e2d45", zerolinecolor="#1e2d45"),
    margin=dict(l=20, r=20, t=40, b=20),
)

TEAL    = "#00d4ff"
ORANGE  = "#ff6b35"
GREEN   = "#10b981"
RED     = "#ff4444"
COLORS  = [TEAL, ORANGE, GREEN, "#a78bfa", "#f59e0b", "#ec4899",
           "#06b6d4", "#84cc16", "#f97316", "#8b5cf6"]


# Data loading
@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(DATABASE_URL)
    fact    = pd.read_sql("SELECT * FROM fact_trade_flows", engine)
    country = pd.read_sql("SELECT * FROM dim_country",     engine)
    material= pd.read_sql("SELECT * FROM dim_material",    engine)
    date    = pd.read_sql("SELECT * FROM dim_date",        engine)
    return fact, country, material, date


# Sidebar section
NAV_PAGES = ["Overview", "Trade Trends", "Supplier Analysis", "Supply Risk"]
NAV_ICONS = {"Overview": "➽", "Trade Trends": "➽", "Supplier Analysis": "➽", "Supply Risk": "➽"}

if "page" not in st.session_state:
    st.session_state.page = "Overview"

with st.sidebar:
    # Avatar / app identity block
    st.markdown("""
    <div class='sb-avatar'>
        <div class='sb-avatar-icon'>🏗️</div>
        <div>
            <div class='sb-avatar-name'>EPC Procurement</div>
            <div class='sb-avatar-sub'>Intelligence Pipeline</div>
        </div>
    </div>
    <div class='sb-divider'></div>
    """, unsafe_allow_html=True)

    # Analytics nav — button-based for session state routing
    # Active page rendered inline with teal overlay via injected HTML wrapper
    st.markdown("<div class='sb-section-label'>Analytics</div>", unsafe_allow_html=True)
    for p in NAV_PAGES:
        # Inject active state as a styled HTML row above the button for active page
        if st.session_state.page == p:
            st.markdown(f"""
            <div style="
                background: rgba(0, 212, 255, 0.15);
                border-radius: 8px;
                padding: 9px 14px;
                font-size: 13px;
                font-weight: 600;
                color: #00d4ff;
                margin-bottom: 2px;
                cursor: default;
            ">{NAV_ICONS[p]}&nbsp;&nbsp;{p}</div>
            """, unsafe_allow_html=True)
        else:
            if st.button(f"{NAV_ICONS[p]}  {p}", key=f"nav_{p}", use_container_width=True):
                st.session_state.page = p
                st.rerun()

    st.markdown("<div class='sb-divider'></div>", unsafe_allow_html=True)

    # Data source info
    st.markdown("<div class='sb-section-label'>Data Source</div>", unsafe_allow_html=True)
    st.markdown("""
    <div class='sb-nav-item'><span class='sb-nav-icon'>➽</span> UN Comtrade · HS84</div>
    <div class='sb-nav-item'><span class='sb-nav-icon'>➽</span> 2026 · Pipeline v1.0</div>
    """, unsafe_allow_html=True)

    st.markdown("<div class='sb-divider'></div>", unsafe_allow_html=True)

    # Stack
    st.markdown("<div class='sb-section-label'>Stack</div>", unsafe_allow_html=True)
    st.markdown("<div class='sb-footer'>⚡ PySpark · PostgreSQL · Streamlit</div>", unsafe_allow_html=True)

# Resolve active page from session state
page = st.session_state.page


# Loading data
try:
    fact, country, material, date = load_data()
except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.stop()



# PAGE 1 — OVERVIEW

if page == "Overview":
    st.markdown("<div class='page-title'>Overview — Oil & Gas Equipment Procurement</div>",
                unsafe_allow_html=True)

    # KPI row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_value = fact["trade_usd_millions"].sum()
        st.metric("Total Import Value", f"${total_value:,.1f}M")
    with col2:
        st.metric("Total Records", f"{len(fact):,}")
    with col3:
        st.metric("Supplier Countries", fact["country_or_area"].nunique())
    with col4:
        st.metric("Material Categories", fact["material_category"].nunique())

    st.markdown("---")

    col_left, col_right = st.columns(2)

    # Top categories by value
    with col_left:
        st.markdown("##### Import Value by Material Category")
        cat_df = fact.groupby("material_category")["trade_usd_millions"] \
                     .sum().reset_index() \
                     .sort_values("trade_usd_millions", ascending=True)
        fig = px.bar(
            cat_df,
            x="trade_usd_millions",
            y="material_category",
            orientation="h",
            color_discrete_sequence=[TEAL]
        )
        fig.update_layout(**PLOTLY_LAYOUT, title="")
        st.plotly_chart(fig, use_container_width=True)

    # Category share pie
    with col_right:
        st.markdown("##### Category Share")
        fig2 = px.pie(
            cat_df,
            values="trade_usd_millions",
            names="material_category",
            color_discrete_sequence=COLORS,
            hole=0.4
        )
        fig2.update_layout(**PLOTLY_LAYOUT)
        fig2.update_traces(textfont_color="#ffffff")
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    st.markdown("##### Data Coverage")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.metric("Year Range", f"{fact['year'].min()} – {fact['year'].max()}")
    with col_b:
        high_risk = (fact["supply_risk_flag"] == "HIGH RISK").sum()
        st.metric("High Risk Records", f"{high_risk:,}")
    with col_c:
        avg_val = fact["trade_usd_millions"].mean()
        st.metric("Avg Trade Value", f"${avg_val:.3f}M")



# PAGE 2 — TRADE TRENDS

elif page == "Trade Trends":
    st.markdown("<div class='page-title'>Trade Trends — Import Volume Over Time</div>",
                unsafe_allow_html=True)

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        categories = ["All"] + sorted(fact["material_category"].unique().tolist())
        selected_cat = st.selectbox("Filter by Material Category", categories)
    with col_f2:
        year_range = st.slider(
            "Year Range",
            int(fact["year"].min()),
            int(fact["year"].max()),
            (int(fact["year"].min()), int(fact["year"].max()))
        )

    # Apply filters
    filtered = fact.copy()
    if selected_cat != "All":
        filtered = filtered[filtered["material_category"] == selected_cat]
    filtered = filtered[filtered["year"].between(year_range[0], year_range[1])]

    st.markdown("---")

    # Yearly trend line
    yearly = filtered.groupby("year")["trade_usd_millions"].sum().reset_index()
    fig = px.line(
        yearly,
        x="year",
        y="trade_usd_millions",
        markers=True,
        color_discrete_sequence=[TEAL]
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=7))
    fig.update_layout(**PLOTLY_LAYOUT,
                      title="Total Import Value by Year (USD Millions)")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Category trend over time
    st.markdown("##### Import Value by Category Over Time")
    cat_year = filtered.groupby(["year", "material_category"])["trade_usd_millions"] \
                       .sum().reset_index()
    fig2 = px.bar(
        cat_year,
        x="year",
        y="trade_usd_millions",
        color="material_category",
        color_discrete_sequence=COLORS,
        barmode="stack"
    )
    fig2.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig2, use_container_width=True)



# PAGE 3 — SUPPLIER ANALYSIS

elif page == "Supplier Analysis":
    st.markdown("<div class='page-title'>Supplier Country Analysis</div>",
                unsafe_allow_html=True)

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        categories = ["All"] + sorted(fact["material_category"].unique().tolist())
        selected_cat = st.selectbox("Filter by Material Category", categories)
    with col_f2:
        years = ["All"] + sorted(fact["year"].unique().tolist(), reverse=True)
        selected_year = st.selectbox("Filter by Year", years)

    filtered = fact.copy()
    if selected_cat != "All":
        filtered = filtered[filtered["material_category"] == selected_cat]
    if selected_year != "All":
        filtered = filtered[filtered["year"] == selected_year]

    st.markdown("---")

    col_left, col_right = st.columns(2)

    # Top countries by value
    with col_left:
        st.markdown("##### Top Supplier Countries by Import Value")
        country_df = filtered.groupby("country_or_area")["trade_usd_millions"] \
                             .sum().reset_index() \
                             .sort_values("trade_usd_millions", ascending=False) \
                             .head(10)
        fig = px.bar(
            country_df,
            x="country_or_area",
            y="trade_usd_millions",
            color_discrete_sequence=[ORANGE]
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

    # Country vs category heatmap
    with col_right:
        st.markdown("##### Country × Category Import Value")
        heatmap_df = filtered.groupby(["country_or_area", "material_category"])["trade_usd_millions"] \
                             .sum().reset_index()
        pivot = heatmap_df.pivot(
            index="country_or_area",
            columns="material_category",
            values="trade_usd_millions"
        ).fillna(0)

        fig2 = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0, "#111827"], [1, TEAL]],
            showscale=True
        ))
        fig2.update_layout(**PLOTLY_LAYOUT)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    st.markdown("##### Full Supplier Data Table")
    st.dataframe(
        country_df.rename(columns={
            "country_or_area": "Country",
            "trade_usd_millions": "Import Value (USD M)"
        }),
        use_container_width=True,
        hide_index=True
    )



# PAGE 4 SUPPLY RISK

elif page == "Supply Risk":
    st.markdown("<div class='page-title'>Supply Risk Analysis</div>",
                unsafe_allow_html=True)

    # Risk summary KPIs
    total     = len(fact["commodity"].unique())
    high_risk = fact[fact["supply_risk_flag"] == "HIGH RISK"]["commodity"].nunique()
    no_risk   = total - high_risk
    pct_risk  = (high_risk / total * 100) if total > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Commodities", total)
    with col2:
        st.metric("High Risk", high_risk, delta="< 3 suppliers")
    with col3:
        st.metric("No Risk", no_risk)
    with col4:
        st.metric("Risk Rate", f"{pct_risk:.1f}%")

    st.markdown("---")

    col_left, col_right = st.columns(2)

    # Risk flag donut
    with col_left:
        st.markdown("##### Risk Distribution")
        risk_df = fact.groupby("supply_risk_flag")["commodity"] \
                      .nunique().reset_index()
        risk_df.columns = ["Risk Flag", "Commodities"]
        fig = px.pie(
            risk_df,
            values="Commodities",
            names="Risk Flag",
            color="Risk Flag",
            color_discrete_map={"HIGH RISK": RED, "NO RISK": GREEN},
            hole=0.5
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_traces(textfont_color="#ffffff")
        st.plotly_chart(fig, use_container_width=True)

    # Supplier count per category
    with col_right:
        st.markdown("##### Avg Supplier Count by Material Category")
        supplier_df = fact.groupby("material_category")["supplier_count"] \
                          .mean().reset_index() \
                          .sort_values("supplier_count", ascending=True)
        colors = [RED if v < 3 else GREEN
                  for v in supplier_df["supplier_count"]]
        fig2 = px.bar(
            supplier_df,
            x="supplier_count",
            y="material_category",
            orientation="h",
            color_discrete_sequence=[TEAL]
        )
        fig2.update_traces(marker_color=colors)
        fig2.update_layout(**PLOTLY_LAYOUT)
        fig2.add_vline(x=3, line_dash="dash", line_color=ORANGE,
                       annotation_text="Risk threshold (3)",
                       annotation_font_color=ORANGE)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # High risk table
    st.markdown("##### ⚠️ High Risk Commodities (< 3 Supplier Countries)")
    high_risk_df = fact[fact["supply_risk_flag"] == "HIGH RISK"][
        ["commodity", "material_category", "supplier_count", "trade_usd_millions"]
    ].drop_duplicates(subset=["commodity"]) \
     .sort_values("trade_usd_millions", ascending=False) \
     .rename(columns={
        "commodity": "Commodity",
        "material_category": "Category",
        "supplier_count": "Supplier Count",
        "trade_usd_millions": "Avg Trade Value (USD M)"
    })

    st.dataframe(high_risk_df, use_container_width=True, hide_index=True)