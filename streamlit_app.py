import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ==============================
# CONFIG
# ==============================
BASE_DIR = Path(".").resolve()
SQLITE_DB_PATH = BASE_DIR / "dashboard_gold.db"

st.set_page_config(
    page_title="Big Data Dashboard (Gold DB)",
    layout="wide"
)

# ==============================
# DB HELPERS
# ==============================
def get_conn():
    if not SQLITE_DB_PATH.exists():
        st.error(f"SQLite DB not found: {SQLITE_DB_PATH}")
        st.stop()
    return sqlite3.connect(SQLITE_DB_PATH)

def read_df(query: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    finally:
        conn.close()

def exec_sql(query: str, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        if params is None:
            cur.execute(query)
        else:
            cur.execute(query, params)
        conn.commit()
    finally:
        conn.close()

def ensure_feedback_table():
    exec_sql("""
        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now')),
            user_name TEXT,
            page TEXT,
            rating INTEGER,
            comment TEXT
        );
    """)

# ==============================
# UI HELPERS
# ==============================
def styled_dataframe(df: pd.DataFrame):
    """Pretty dataframe with basic styling."""
    if df.empty:
        st.info("No rows to display.")
        return

    # Try to highlight numeric columns (optional)
    styler = df.style
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        styler = styler.background_gradient(subset=num_cols)

    st.dataframe(styler, use_container_width=True)

# ==============================
# LOAD TABLES
# ==============================
@st.cache_data(show_spinner=False)
def load_inventory():
    return read_df("SELECT * FROM gold_inventory ORDER BY table_name;")

@st.cache_data(show_spinner=False)
def load_q1():
    return read_df("SELECT * FROM q1_pareto_analysis ORDER BY total_sales DESC;")

@st.cache_data(show_spinner=False)
def load_q2():
    return read_df("SELECT * FROM q2_top_products_city ORDER BY city, rank_in_city;")

@st.cache_data(show_spinner=False)
def load_q3():
    return read_df("SELECT * FROM q3_basket_size_analysis ORDER BY city_rank;")

@st.cache_data(show_spinner=False)
def load_q4():
    return read_df("SELECT * FROM q4_holiday_impact ORDER BY local_holiday_rank;")

@st.cache_data(show_spinner=False)
def load_q5():
    return read_df("SELECT * FROM q5_sales_monthly_pivot ORDER BY year;")

@st.cache_data(show_spinner=False)
def load_q6():
    return read_df("SELECT * FROM q6_perishable_growth;")

# ==============================
# PAGES
# ==============================
def page_overview():
    st.title("Overview")
    st.caption(f"Data source: {SQLITE_DB_PATH}")

    inv = load_inventory()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Gold tables", int((inv["table_name"].str.startswith("q")).sum()))
    with c2:
        st.metric("Sample tables", int((inv["table_name"].str.startswith("sample_")).sum()))
    with c3:
        st.metric("Total rows (all tables)", int(inv["row_count"].sum()))

    st.subheader("Gold Inventory (Tables Summary)")
    styled_dataframe(inv)

def page_q1_pareto():
    st.title("Q1: Pareto Analysis (80/20)")
    df = load_q1()

    # Interactive filter
    families = ["(All)"] + sorted(df["family"].dropna().unique().tolist())
    selected_family = st.selectbox("Filter by family", families)

    if selected_family != "(All)":
        df = df[df["family"] == selected_family]

    st.subheader("Top Items by Revenue Contribution")
    styled_dataframe(df.head(50))

    # Matplotlib chart: cumulative % over rank
    st.subheader("Cumulative Revenue Curve (Top 500)")
    plot_df = df.head(500).copy()
    plot_df["rank"] = range(1, len(plot_df) + 1)

    fig = plt.figure()
    plt.plot(plot_df["rank"], plot_df["cumulative_pct"])
    plt.xlabel("Item rank (by total_sales)")
    plt.ylabel("Cumulative % of revenue")
    plt.title("Pareto Curve")
    st.pyplot(fig)

def page_q2_city_preferences():
    st.title("Q2: Regional Preferences (Top 3 families per city)")
    df = load_q2()

    cities = ["(All)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Choose city", cities)

    if selected_city != "(All)":
        df = df[df["city"] == selected_city]

    st.subheader("Top Families")
    styled_dataframe(df)

    # Quick chart: if a single city chosen, plot top3
    if selected_city != "(All)" and not df.empty:
        st.subheader(f"Top 3 in {selected_city} (bar)")
        fig = plt.figure()
        plt.bar(df["family"], df["total_sold"])
        plt.xlabel("Family")
        plt.ylabel("Total sold")
        plt.title("Top-3 families")
        st.pyplot(fig)

def page_q3_basket_size():
    st.title("Q3: Basket Size (Items per Transaction)")
    df = load_q3()

    top_n = st.slider("Show top N cities", min_value=5, max_value=22, value=10, step=1)
    st.subheader(f"Top {top_n} Cities by Avg Basket Size")
    styled_dataframe(df.head(top_n))

    st.subheader("Avg Basket Size Distribution (Top N)")
    plot_df = df.head(top_n)
    fig = plt.figure()
    plt.bar(plot_df["city"], plot_df["avg_basket_size"])
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("City")
    plt.ylabel("Avg basket size")
    plt.title("Basket size by city")
    st.pyplot(fig)

def page_q4_holidays():
    st.title("Q4: Local vs National Holidays Impact")
    df = load_q4()

    winner = st.radio("Show cities where winner is:", ["All", "Local", "National"], horizontal=True)
    if winner != "All":
        df = df[df["winner_type"] == winner]

    st.subheader("Holiday Impact Table")
    styled_dataframe(df)

    st.subheader("Scatter: Local vs National Avg")
    fig = plt.figure()
    plt.scatter(df["national_holiday_avg"], df["local_holiday_avg"])
    plt.xlabel("National holiday avg sales")
    plt.ylabel("Local holiday avg sales")
    plt.title("Local vs National holiday sales")
    st.pyplot(fig)

def page_q5_seasonality():
    st.title("Q5: Seasonality (Monthly Sales by Year)")
    df = load_q5()

    years = sorted(df["year"].dropna().unique().tolist())
    selected_years = st.multiselect("Select years", years, default=years)

    df = df[df["year"].isin(selected_years)]
    st.subheader("Pivot Table")
    styled_dataframe(df)

    # Line chart by month (average across selected years)
    st.subheader("Average Monthly Sales (selected years)")
    month_cols = [str(i) for i in range(1, 13)]
    # Columns are numeric in SQLite -> pandas may read as ints; handle both
    existing_cols = [c for c in df.columns if str(c) in month_cols]
    if existing_cols:
        avg_series = df[existing_cols].mean(numeric_only=True)
        # sort months
        avg_series.index = [int(str(x)) for x in avg_series.index]
        avg_series = avg_series.sort_index()

        fig = plt.figure()
        plt.plot(avg_series.index, avg_series.values)
        plt.xlabel("Month")
        plt.ylabel("Avg sales")
        plt.title("Seasonality curve")
        st.pyplot(fig)

def page_q6_perishables():
    st.title("Q6: Perishable Growth (YoY) - Top Stores")
    df = load_q6()

    cities = ["(All)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Filter by city", cities)

    if selected_city != "(All)":
        df = df[df["city"] == selected_city]

    st.subheader("Perishable Growth Table")
    styled_dataframe(df)

    st.subheader("Top Growth % (bar)")
    plot_df = df.sort_values("growth_pct", ascending=False).head(10)
    fig = plt.figure()
    plt.bar(plot_df["city"], plot_df["growth_pct"])
    plt.xlabel("City")
    plt.ylabel("Growth %")
    plt.title("Top growth % (filtered)")
    st.pyplot(fig)

def page_feedback():
    st.title("Feedback")
    ensure_feedback_table()

    with st.form("feedback_form"):
        user_name = st.text_input("Name (optional)")
        page = st.selectbox("Which page is this about?", [
            "Overview", "Q1 Pareto", "Q2 City Preferences", "Q3 Basket Size",
            "Q4 Holidays", "Q5 Seasonality", "Q6 Perishables", "General"
        ])
        rating = st.slider("Rating (1-5)", 1, 5, 5)
        comment = st.text_area("Comment", placeholder="What worked? What should be improved?")
        submitted = st.form_submit_button("Submit")

    if submitted:
        exec_sql(
            "INSERT INTO feedback(user_name, page, rating, comment) VALUES (?, ?, ?, ?);",
            (user_name, page, int(rating), comment)
        )
        st.success("Thanks! Feedback saved to SQLite.")

    st.subheader("Latest feedback")
    fb = read_df("SELECT created_at, user_name, page, rating, comment FROM feedback ORDER BY id DESC LIMIT 20;")
    styled_dataframe(fb)

# ==============================
# NAV
# ==============================
PAGES = {
    "Overview": page_overview,
    "Q1 Pareto": page_q1_pareto,
    "Q2 City Preferences": page_q2_city_preferences,
    "Q3 Basket Size": page_q3_basket_size,
    "Q4 Holidays": page_q4_holidays,
    "Q5 Seasonality": page_q5_seasonality,
    "Q6 Perishables": page_q6_perishables,
    "Feedback": page_feedback,
}

st.sidebar.title("Navigation")
choice = st.sidebar.radio("Go to", list(PAGES.keys()))
PAGES[choice]()
