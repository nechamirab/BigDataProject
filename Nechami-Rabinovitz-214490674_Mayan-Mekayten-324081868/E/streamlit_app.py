import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from wordcloud import WordCloud
import plotly.express as px

# ==============================
# CONFIGURATION
# ==============================
BASE_DIR = Path(".").resolve()
SQLITE_DB_PATH = BASE_DIR / "dashboard_gold.db"

st.set_page_config(
    page_title="Dashboard - Project",
    layout="wide"
)

# Set Seaborn theme
sns.set_theme(style="whitegrid")


# ==============================
# DATABASE HELPERS
# ==============================
def _assert_db_exists():
    """Checks if the SQLite database file exists."""
    if not SQLITE_DB_PATH.exists():
        st.error(f"SQLite file not found: {SQLITE_DB_PATH}")
        st.stop()


def get_conn():
    """Establishes a connection to the SQLite database."""
    _assert_db_exists()
    return sqlite3.connect(SQLITE_DB_PATH)


def read_df(query: str, params=None) -> pd.DataFrame:
    """Executes a SQL query and returns a Pandas DataFrame."""
    conn = get_conn()
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def exec_sql(query: str, params=None):
    """Executes a SQL command (INSERT, UPDATE, CREATE)."""
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
    """Creates the feedback table if it does not exist."""
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now')),
            user_name TEXT,
            page TEXT,
            rating INTEGER,
            comment TEXT
        );
        """
    )

# ==============================
# UI HELPERS
# ==============================
def explain_box(title: str, text: str):
    """Top-of-page explanation box."""
    st.markdown(
        f"""
        <div style="
            border: 1px solid #ddd;
            border-radius: 12px;
            padding: 14px 16px;
            background: #fafafa;
            margin-bottom: 14px;">
            <div style="font-weight: 700; font-size: 18px; margin-bottom: 6px;">{title}</div>
            <div style="font-size: 15px; line-height: 1.6;">{text}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def styled_dataframe(df: pd.DataFrame, height: int = 420):
    """Renders a DataFrame with a gradient background style."""
    if df is None or df.empty:
        st.info("There is no data to display (the table is empty or the filters have filtered everything out).")
        return

    styler = df.style
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        # Gradient background for numerical columns
        styler = styler.background_gradient(subset=num_cols, cmap="Blues")

    st.dataframe(styler, use_container_width=True, height=height)


def rename_columns_for_display(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Renames columns from English to Hebrew for display purposes."""
    if df is None or df.empty:
        return df
    cols = {c: mapping[c] for c in df.columns if c in mapping}
    return df.rename(columns=cols)


# ==============================
# PLOTTING FUNCTIONS (Original)
# ==============================
def safe_bar_chart(x, y, title, xlabel, ylabel, rotate_xticks=False):
    """Generates a Matplotlib bar chart and renders it in Streamlit."""
    fig = plt.figure(figsize=(10, 5))
    plt.bar(x, y, color='pink', label=ylabel)
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend()
    if rotate_xticks:
        plt.xticks(rotation=45, ha="right")
    st.pyplot(fig)


def safe_line_chart(x, y, title, xlabel, ylabel):
    """Generates a Matplotlib line chart and renders it in Streamlit."""
    fig = plt.figure(figsize=(10, 5))
    plt.plot(x, y, marker='o', linestyle='-', color='purple', label=ylabel)
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    st.pyplot(fig)


def safe_scatter(x, y, title, xlabel, ylabel):
    """Generates a Matplotlib scatter plot with a diagonal reference line."""
    fig = plt.figure(figsize=(8, 6))
    plt.scatter(x, y, alpha=0.7)

    # Add diagonal line for reference
    lims = [
        min(plt.xlim()[0], plt.ylim()[0]),
        max(plt.xlim()[1], plt.ylim()[1]),
    ]
    plt.plot(lims, lims, 'r--', alpha=0.5)

    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    st.pyplot(fig)


# ==============================
# LOAD TABLES (cached)
# ==============================
@st.cache_data(show_spinner=False)
def load_inventory():
    return read_df("SELECT * FROM gold_inventory ORDER BY table_name;")

@st.cache_data(show_spinner=False)
def load_table(table_name):
    """Generic loader for raw sample tables."""
    return read_df(f"SELECT * FROM {table_name}")

@st.cache_data(show_spinner=False)
def load_q1():
    return read_df("SELECT * FROM q1_pareto_analysis ORDER BY total_sales DESC;")

@st.cache_data(show_spinner=False)
def load_q2():
    return read_df("SELECT * FROM q2_perishable_growth;")

@st.cache_data(show_spinner=False)
def load_q3():
    return read_df("SELECT * FROM q3_top_products_city ORDER BY city, rank_in_city;")

@st.cache_data(show_spinner=False)
def load_q4():
    return read_df("SELECT * FROM q4_basket_size_analysis ORDER BY city_rank;")

@st.cache_data(show_spinner=False)
def load_q5():
    return read_df("SELECT * FROM q5_holiday_impact ORDER BY local_holiday_rank;")

@st.cache_data(show_spinner=False)
def load_q6():
    return read_df("SELECT * FROM q6_geo_cube;")

@st.cache_data(show_spinner=False)
def load_q7():
    return read_df("SELECT * FROM q7_sales_monthly_pivot ORDER BY year;")

@st.cache_data(show_spinner=False)
def load_q8():
    return read_df("SELECT * FROM q8_oil_sales ORDER BY month;")


# ==============================
# DISPLAY COLUMN MAPPINGS
# ==============================
MAP_INVENTORY = {
    "table_name": "Table name",
    "row_count": "Number of rows",
    "col_count": "Number of columns",
}
MAP_Q1 = {
    "item_nbr": "Item ID",
    "family": "Category",
    "total_sales": "Total sales",
    "cumulative_pct": "Cumulative percentage",
    "pareto_group": "Pareto group",
}

MAP_Q2 = {
    "city": "City",
    "sales_year": "Year",
    "current_sales": "Current year sales",
    "previous_sales": "Previous year sales",
    "growth_pct": "percent growth",
}

MAP_Q3 = {
    "city": "City",
    "family": "Category",
    "total_sold": "Total sold",
    "rank_in_city": "Rank in city",
}
MAP_Q4 = {
    "city": "City",
    "total_items_sold": "Total items sold",
    "total_transactions": "Total transactions",
    "avg_basket_size": "Average basket size",
    "city_rank": "City rank",
}
MAP_Q5 = {
    "city": "City",
    "national_holiday_avg": "National holiday average",
    "local_holiday_avg": "Local holiday average",
    "winner_type": "The winner",
    "local_holiday_rank": "Local holiday rank",
}

MAP_Q6 = {
    "state": "State / Province",
    "city": "City",
    "total_sales": "Total Sales",
    "aggregation_level": "View Level"
}

MAP_Q8 = {
    "month": "Month",
    "total_sales": "Total Sales",
    "avg_oil_price": "Avg Oil Price ($)"
}

# ==============================
# PAGES
# ==============================
def page_overview():
    """
    Renders the Overview page.
    Displays a summary of the project data, including the number of tables and total row counts.
    """
    st.title("Overview 👀")
    explain_box(
        "About the Dashboard",
        "This dashboard presents business insights derived from large-scale retail data. The data has been processed (ETL) and stored in a GOLD layer (SQLite) for optimal performance.\n\n"
        "**What's on this page:** A summary of the available data tables, including row counts and table types (Analysis vs. Samples)."
    )
    st.caption(f"Data source: {SQLITE_DB_PATH}")

    inv = load_inventory()
    if inv is None or inv.empty:
        st.error("The gold_inventory table was not found or is empty. You must have successfully run stage_e_analysis.py.")
        return

    gold_count = int((inv["table_name"].astype(str).str.startswith("q")).sum())
    sample_count = int((inv["table_name"].astype(str).str.startswith("sample_")).sum())
    total_rows = int(inv["row_count"].sum())

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Number of GOLD tables", gold_count)
    with c2:
        st.metric("Number of SAMPLE tables", sample_count)
    with c3:
        st.metric("Total rows in all tables", f"{total_rows:,}")

    st.subheader("Table inventory (gold_inventory)")
    inv_disp = rename_columns_for_display(inv, MAP_INVENTORY)
    styled_dataframe(inv_disp, height=520)


def page_raw_data():
    """
    Renders the Raw Data page.
    Allows the user to select and preview sample data from the source tables.
    """
    st.title("Raw data (samples) 📚")
    explain_box("Data Exploration",
        "This page allows you to inspect the structure of the original data tables before any aggregation.\n\n"
             "**What's on this page:** A dropdown menu to select a table (e.g., Train, Items, Stores) and a view of 150 sample rows from that table."
    )

    inv = load_inventory()
    if inv is not None and not inv.empty:
        # Filter for tables starting with "sample_"
        sample_tables = inv[inv["table_name"].astype(str).str.startswith("sample_")]["table_name"].tolist()

        if sample_tables:
            selected_table = st.selectbox("Select table:", sample_tables)

            if selected_table:
                df = load_table(selected_table)
                st.subheader(f"Preview: {selected_table}")
                st.write(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
                styled_dataframe(df, height=600)
        else:
            st.warning("No sample tables (sample_*) found in the database.")
    else:
        st.error("Error loading inventory table.")


def page_q1_pareto():
    """
    Renders the Q1: Pareto Analysis page.
    Visualizes the 80/20 rule to identify core revenue products.
    """
    st.title("Q1: Pareto Analysis (80/20) 💰")
    explain_box(
        "Inventory Efficiency",
        "This analysis checks if the '80/20 rule' applies here: Does a small number of products generate the majority (80%) of the revenue?\n\n"
             "**What's on this page:** A table showing products sorted by sales with their Pareto group classification, and a line chart visualizing the cumulative sales curve."
    )

    df = load_q1()
    if df is None or df.empty:
        st.error("q1_pareto_analysis does not exist/is empty in SQLite.")
        return

    families = ["(All)"] + sorted(df["family"].dropna().unique().tolist())
    selected_family = st.selectbox("Filter by Category", families)

    filtered = df.copy()
    if selected_family != "(All)":
        filtered = filtered[filtered["family"] == selected_family]

    st.subheader("Quick Insight")
    core_items = (filtered["pareto_group"].astype(str).str.startswith("Top")).sum()
    total_items = len(filtered)
    core_pct = (core_items / total_items) * 100 if total_items else 0
    st.write(f"Out of **{total_items:,}** products, approximately **{core_pct:.2f}%** are in the 'Top 80% (Core Revenue)' group.")

    st.subheader("Top Products Table (Top 50)")
    disp = rename_columns_for_display(filtered, MAP_Q1)
    styled_dataframe(disp.head(50))

    st.subheader("Pareto Curve")
    plot_df = filtered.head(500).copy()
    plot_df["rank"] = range(1, len(plot_df) + 1)

    safe_line_chart(
        x=plot_df["rank"],
        y=plot_df["cumulative_pct"],
        title="Pareto Curve",
        xlabel="Item Rank (by Total Sales)",
        ylabel="Cumulative % of Sales"
    )

    st.markdown(
        "**Chart Description:** The X-axis displays the product ranking by sales (from best-selling to least), "
        "and the Y-axis shows the **Cumulative Sales %**. "
        "A steep rise at the beginning indicates that a few products generate a large portion of the sales."
    )

def page_q2_perishables():
    """
    Renders the Q2: Perishable Goods Growth page.
    Analyzes Year-Over-Year (YoY) growth for perishable items across cities.
    """
    st.title("Q2: Perishable Goods Growth (YoY) ♻️")
    explain_box(
        "Year-Over-Year (YoY) Growth",
        "Which cities are showing the highest growth in the sales of perishable goods?\n\n"
        "**What's on this page:** A bar chart showing the top 20 cities by growth percentage, and a comparison table."
    )

    df = load_q2()
    if df is None or df.empty:
        st.error("q2_perishable_growth does not exist/is empty in SQLite.")
        return

    avg_growth = df["growth_pct"].mean()
    best_city_row = df.sort_values("growth_pct", ascending=False).iloc[0]

    st.subheader("Key Performance Indicators")
    k1, k2, k3 = st.columns(3)
    k1.metric("Average Market Growth", f"{avg_growth:.1f}%")
    k2.metric("Fastest Growing City", best_city_row["city"])
    k3.metric("Highest Growth Rate", f"{best_city_row['growth_pct']:.1f}%")
    st.divider()

    cities = ["(All)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Filter by City", cities)

    filtered = df.copy()
    if selected_city != "(All)":
        filtered = filtered[filtered["city"] == selected_city]

    st.subheader("YoY Growth Table")
    disp = rename_columns_for_display(filtered, MAP_Q2)
    styled_dataframe(disp, height=520)

    st.subheader("Chart: Top 20 by Growth %")
    plot_df = filtered.sort_values("growth_pct", ascending=False).head(10)
    if plot_df.empty:
        st.info("No data for chart based on current filter.")
        return

    safe_bar_chart(
        x=plot_df["city"],
        y=plot_df["growth_pct"],
        title="Top 20 Growth Percentage (YoY)",
        xlabel="City",
        ylabel="Growth %"
    )
    st.markdown(
        "**Chart Description:** A bar chart showing the **Top 20 Cities by YoY Growth %** "
        "based on the current filter. A taller bar represents higher growth compared to the previous year. "
        "This helps identify hotspots where the demand for perishable goods is rising rapidly."
    )


def page_q3_city_preferences():
    """
    Renders the Q3: Regional Preferences page.
    Identifies top selling product categories in each city using bars and word clouds.
    """
    st.title("Q3: Regional Preferences (Top-3) ⛰️")
    explain_box(
        "Localization Analysis",
        "What are the top 3 selling product categories in each city? This helps understand local consumer behavior.\n\n"
             "**What's on this page:** A city selector, a bar chart displaying the top 3 categories for the selected city, and a detailed table of rankings."
    )

    df = load_q3()
    if df is None or df.empty:
        st.error("q3_top_products_city does not exist/is empty in SQLite.")
        return

    cities = ["(All)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Select City", cities)

    filtered = df.copy()
    if selected_city != "(All)":
        filtered = filtered[filtered["city"] == selected_city]

    st.subheader(f"Top Categories in {selected_city}")
    disp = rename_columns_for_display(filtered, MAP_Q3)
    styled_dataframe(disp, height=520)

    if selected_city != "(All)" and not filtered.empty:
        st.subheader(f"Chart: Top-3 in {selected_city}")
        safe_bar_chart(
            x=filtered["family"],
            y=filtered["total_sold"],
            title="Top-3 Product Families by City",
            xlabel="Category",
            ylabel="Total Sold",
            rotate_xticks=True
        )
        st.markdown(
            "**Chart Description:** A bar chart showing the **Top 3 leading product categories** in the selected city, "
            "where the height of each bar represents the **total units sold** for that category. "
            "This helps identify the specific consumer preferences of the city."
        )

    st.divider()

    if selected_city == "(All)":
        wc_title = "Global Dominance (All Cities)"
    else:
        wc_title = f"Winning Categories in {selected_city}"

    st.subheader(f"{wc_title} (Word Cloud) ☁️")
    wordcloud_data = filtered.groupby("family")["total_sold"].sum().to_dict()

    if wordcloud_data:
        wc = WordCloud(
            width=900,
            height=350,
            background_color='white',
            colormap='viridis',
        ).generate_from_frequencies(wordcloud_data)

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.imshow(wc, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)
        plt.close(fig)
    else:
        st.info("Not enough data to generate Word Cloud.")


def page_q4_basket_size():
    """
    Renders the Q4: Basket Size Analysis page.
    Compares the average number of items per transaction across cities.
    """
    st.title("Q4: Average Basket Size 🛒")
    explain_box(
        "Store Efficiency",
        "Which cities have the largest average basket size (items per transaction)?\n\n"
        "**What's on this page:** A slider to choose the number of top cities, a bar chart comparing basket sizes, and a ranked table."
    )

    df = load_q4()
    if df is None or df.empty:
        st.error("q4_basket_size_analysis does not exist/is empty in SQLite.")
        return

    max_n = max(5, min(22, len(df)))
    top_n = st.slider("Select Top N Cities", min_value=5, max_value=max_n, value=min(10, max_n), step=1)

    top_df = df.head(top_n).copy()

    st.subheader(f"Top {top_n} Cities by Basket Size")
    disp = rename_columns_for_display(top_df, MAP_Q4)
    styled_dataframe(disp)

    st.subheader("Chart: Basket Size by City (Top-N)")
    safe_bar_chart(
        x=top_df["city"],
        y=top_df["avg_basket_size"],
        title="Average Basket Size by City",
        xlabel="City",
        ylabel="Avg Basket Size",
        rotate_xticks=True
    )
    st.markdown(
        "**Chart Description:** A bar chart displaying the **Average Basket Size** (items per transaction) "
        "for each city (in the Top-N). A taller bar indicates that, on average, customers in that city "
        "purchase more items per visit."
    )


def page_q5_holidays():
    """
    Renders the Q5: Holiday Impact page.
    Visualizes the difference between Local and National holiday sales.
    """
    st.title("Q5: Local vs. National Holidays 🎊")
    explain_box(
        "Holiday Impact Analysis",
        "Do local holidays generate more sales than national holidays in specific cities?\n\n"
        "**What's on this page:** A scatter plot where each point represents a city, and a comparison table. Points above the diagonal line indicate stronger Local holidays."
    )

    df = load_q5()
    if df is None or df.empty:
        st.error("q5_holiday_impact does not exist/is empty in SQLite.")
        return

    choice = st.radio("Filter by Winner", ["All", "Local", "National"], horizontal=True)
    filtered = df.copy()

    if choice == "Local":
        filtered = filtered[filtered["winner_type"] == "Local"]
    elif choice == "National":
        filtered = filtered[filtered["winner_type"] == "National"]

    st.subheader("Holiday Impact Table")
    disp = rename_columns_for_display(filtered, MAP_Q5)
    styled_dataframe(disp, height=520)

    st.subheader("Chart: Local vs National (Scatter)")

    fig = px.scatter(
        filtered,
        x='national_holiday_avg',
        y='local_holiday_avg',
        color='winner_type',
        hover_name='city',
        size='local_holiday_avg',
        title="Local vs National Sales (Hover for details)",
        labels={
            "national_holiday_avg": "National Holiday Sales",
            "local_holiday_avg": "Local Holiday Sales",
            "winner_type": "Winner"
        },
        template="plotly_white"
    )

    min_val = min(filtered['national_holiday_avg'].min(), filtered['local_holiday_avg'].min())
    max_val = max(filtered['national_holiday_avg'].max(), filtered['local_holiday_avg'].max())

    fig.add_shape(
        type="line", line=dict(dash="dash", color="gray", width=1),
        x0=min_val, y0=min_val, x1=max_val, y1=max_val
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        "**Chart Description:** Each point represents a city. The X-axis is **Average National Holiday Sales**, "
        "and the Y-axis is **Average Local Holiday Sales**. "
        "Points appearing *above the diagonal line* (Y>X) indicate that Local holidays generate more sales in that city."
    )
def page_q6_cube():
    """
    Renders the Q6: Geographic Hierarchy page using CUBE logic.
    Allows drill-down from National -> State -> City levels.
    """
    st.title("Q6: Geographic Hierarchy (CUBE) 🧊")
    explain_box(
        "Multi-Level Aggregation",
        "Using the SQL CUBE function, we analyzed sales across different geographic levels simultaneously:\n"
        "1. National Level (Grand Total)\n"
        "2. State Level (Subtotals by Province)\n"
        "3. City Level (Detailed Sales)\n\n"
        "**What's on this page:** A hierarchical drill-down view of sales performance."
    )

    df = load_q6()
    if df is None or df.empty:
        st.error("q6_geo_cube does not exist in SQLite.")
        return

    # --- National KPI ---
    # Extract the 'Grand Total' row generated by the CUBE function (where both State and City are NULL/Aggregated)
    national_data = df[df["aggregation_level"] == "National Level"]
    if not national_data.empty:
        total_sales = national_data.iloc[0]["total_sales"]
        st.metric("National Total Sales", f"${total_sales:,.0f}")

    st.divider()

    # --- Drill-Down Mechanism ---
    st.subheader("Explore Sales by Level")

    # elect visualization level
    view_mode = st.radio("Select View Level:",
                         ["State Overview", "City Details"],
                         horizontal=True)

    if view_mode == "State Overview":
        # Show only rows representing State-level subtotals
        filtered = df[df["aggregation_level"] == "State Level"].copy()

        st.caption("Showing total sales per State (Province).")
        safe_bar_chart(
            filtered.head(10)["state"],
            filtered.head(10)["total_sales"],
            "Top 10 States by Sales", "State", "Sales", rotate_xticks=True
        )

        disp = rename_columns_for_display(filtered, MAP_Q6)
        styled_dataframe(disp, height=400)

    elif view_mode == "City Details":
        # Users select a State to see its constituent cities
        states = sorted(df[df["aggregation_level"] == "State Level"]["state"].unique())
        selected_state = st.selectbox("Select a State to drill down:", states)

        # Get only 'City Level' rows belonging to the selected State
        filtered = df[
            (df["aggregation_level"] == "City Level") &
            (df["state"] == selected_state)
            ].copy()

        st.caption(f"Showing breakdown of cities within **{selected_state}**.")

        # Display Data
        if not filtered.empty:
            safe_bar_chart(
                filtered["city"],
                filtered["total_sales"],
                f"Sales by City in {selected_state}", "City", "Sales", rotate_xticks=True
            )
            disp = rename_columns_for_display(filtered, MAP_Q6)
            styled_dataframe(disp)
        else:
            st.warning("No city data found for this state.")

def page_q7_seasonality():
    """
    Renders the Q7: Seasonality page.
    Displays monthly sales trends and pivots by year.
    """
    st.title("Q7: Seasonality (Pivot) 📈")
    explain_box(
        "Monthly Sales Trends",
        "This page analyzes sales trends over time, aggregated by month and year, to identify seasonal peaks.\n\n"
        "**What's on this page:** A filter for years, a line chart showing monthly sales, and a Pivot table."
    )

    df = load_q7()
    if df is None or df.empty:
        st.error("q7_sales_monthly_pivot does not exist/is empty in SQLite.")
        return

    years = sorted(df["year"].dropna().unique().tolist())
    selected_years = st.multiselect("Select Years", years, default=years)
    filtered = df[df["year"].isin(selected_years)].copy()

    st.subheader("Pivot Table (Year × Month)")
    styled_dataframe(filtered, height=380)

    st.subheader("Chart: Monthly Average (selected years)")
    month_cols = [c for c in filtered.columns if str(c) in [str(i) for i in range(1, 13)]]
    if not month_cols:
        st.info("No month columns (1..12) found in table q7_sales_monthly_pivot.")
        return

    avg_series = filtered[month_cols].mean(numeric_only=True)
    avg_series.index = [int(str(x)) for x in avg_series.index]
    avg_series = avg_series.sort_index()

    safe_line_chart(
        x=avg_series.index,
        y=avg_series.values,
        title="Seasonality: Monthly Average Sales",
        xlabel="Month",
        ylabel="Average Sales"
    )
    st.markdown(
        "**Chart Description:** A line chart showing the **Average Sales per Month** (1–12) across the selected years. "
        "Peaks or troughs indicate seasonality — specific months that consistently see higher or lower sales."
    )

def page_q8_oil():
    """
    Renders the Q8: Macro Analysis page (Oil Price vs. Sales).
    Merges external macroeconomic data (Oil) with internal sales data.
    Calculates the Pearson correlation coefficient.
    Uses a Dual-Axis chart to plot two variables with different scales.
    """
    st.title("Q8: Macro Analysis - Oil vs. Sales 🛢️")
    explain_box(
        "Economic Correlation",
        "Ecuador's economy is heavily dependent on oil exports. \n"
        "This analysis investigates the correlation between Global Oil Prices and Retail Sales.\n\n"
        "**What's on this page:** A dual-axis time series chart and a correlation coefficient score."
    )

    df = load_q8()
    if df is None or df.empty:
        st.error("q8_oil_sales does not exist in SQLite.")
        return

    # Data Type Conversion: Ensure datetime format for proper plotting
    df["month"] = pd.to_datetime(df["month"])

    # Calculate Pearson correlation coefficient between Sales and Oil Price
    correlation = df["total_sales"].corr(df["avg_oil_price"])

    st.subheader("Correlation Analysis")
    c1, c2 = st.columns([1, 3])
    with c1:
        st.metric("Correlation Coefficient", f"{correlation:.2f}")
    with c2:
        # Interpret the correlation strength
        if correlation > 0.5:
            st.success("Strong Positive Correlation: When oil goes up, sales go up.")
        elif correlation < -0.5:
            st.error("Strong Negative Correlation: When oil goes down, sales go up.")
        else:
            st.info("Weak/No Correlation: Oil prices do not significantly impact grocery sales.")

    st.divider()

    # --- Dual-Axis Chart Visualizatio ---
    st.subheader("Dual-Axis Trends")
    # Create figure and primary axis (Sales)
    fig, ax1 = plt.subplots(figsize=(10, 5))
    # Axis 1: Total Sales (Blue)
    color1 = 'tab:blue'
    ax1.set_xlabel('Date (Month)')
    ax1.set_ylabel('Total Sales', color=color1, fontsize=12)

    line1 = ax1.plot(df["month"], df["total_sales"], color=color1, linewidth=2, label="Sales")
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.grid(False)  # Disable grid to maintain clarity with dual axis

    # Axis 2: Oil Price (Red) - using twinx() to share the X-axis
    ax2 = ax1.twinx()
    color2 = 'tab:red'
    ax2.set_ylabel('Avg Oil Price ($)', color=color2, fontsize=12)
    line2 = ax2.plot(df["month"], df["avg_oil_price"], color=color2, linestyle='--', linewidth=2, label="Oil Price")
    ax2.tick_params(axis='y', labelcolor=color2)

    # Logic to combine both lines into one single Legend box
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="best")

    plt.title("Sales Volume vs. Oil Price Over Time", fontsize=14)
    st.pyplot(fig)

    st.markdown(
        "**Chart Description:** The **Blue Line** represents monthly sales volume, while the **Red Dashed Line** represents the average oil price. "
        "A dual-axis chart allows us to compare trends despite the different scales (Millions of units vs. Dollars)."
    )

    # Display detailed data table formatted nicely
    disp = rename_columns_for_display(df, MAP_Q8)
    styler = disp.style.format({
        "Month": lambda x: x.strftime("%Y-%m"),
        "Total Sales": "{:,.0f}",
        "Avg Oil Price ($)": "${:.2f}"
    })

    st.dataframe(styler, use_container_width=True, height=400)

def page_feedback():
    """
    Renders the Feedback page.
    Allows users to submit feedback and Admin users to manage/delete feedback.
    """
    st.title("User Feedback 💬")
    explain_box(
        "We value your feedback",
        "Please rate the dashboard and provide your comments. Feedback is saved to the SQLite database."
    )
    ensure_feedback_table()

    admin_password = "12345"

    # --- Session State Management for Admin Login ---
    if "admin_logged_in" not in st.session_state:
        st.session_state["admin_logged_in"] = False

    # --- Section 1: Submit Feedback (Write to DB) ---
    st.subheader("Add New Feedback")
    with st.form("feedback_form", clear_on_submit=True):
        user_name = st.text_input("Name (Optional)")

        page = st.selectbox("Which page are you reviewing?", [
            "Overview",
            "Raw Data Samples",
            "Q1: Pareto (80/20)",
            "Q2: Growth (YoY)",
            "Q3: City Preferences",
            "Q4: Basket Size",
            "Q5: Holiday Impact",
            "Q6: Geo Hierarchy",
            "Q7: Seasonality",
            "Q8: Oil & Economy",
            "General"
        ])
        rating = st.radio("Rating (1-5)", [1, 2, 3, 4, 5], index=4, horizontal=True)
        comment = st.text_area("Comments", placeholder="What worked well? What to improve?")

        submitted = st.form_submit_button("Submit")

    if submitted:
        exec_sql(
            "INSERT INTO feedback(user_name, page, rating, comment) VALUES (?, ?, ?, ?);",
            (user_name, page, int(rating), comment)
        )
        st.success("Feedback saved successfully ✅")
        st.rerun()

    st.divider()

    # --- Section 2: View Feedback ---
    st.subheader("Recent Feedback")
    fb = read_df("SELECT id, created_at, user_name, page, rating, comment FROM feedback ORDER BY id DESC;")

    if fb is not None and not fb.empty:
        display_fb = fb.rename(columns={
            "id": "ID",
            "created_at": "Date",
            "user_name": "Name",
            "page": "Page",
            "rating": "Rating",
            "comment": "Comment"
        })
        st.dataframe(display_fb.set_index("ID"), use_container_width=True, height=300)
    else:
        st.info("No feedback available yet.")

    # --- Section 3: Admin Area (Delete & Maintain) ---
    st.markdown("---")
    with st.expander("🔒 Admin Area"):

        # State A: Not Logged In
        if not st.session_state["admin_logged_in"]:
            password_input = st.text_input("Enter Admin Password:", type="password")
            if st.button("Login"):
                if password_input == admin_password:
                    st.session_state["admin_logged_in"] = True
                    st.success("Logged in successfully!")
                    st.rerun()
                else:
                    st.error("Incorrect Password ❌")

        # State B: Logged In
        else:
            # Logout Button
            col_logout, _ = st.columns([1, 4])
            with col_logout:
                if st.button("🚪 Log Out"):
                    st.session_state["admin_logged_in"] = False
                    st.rerun()

            st.success("Access Granted - Admin Mode Active 🔓")

            if fb is not None and not fb.empty:
                st.write("Select a feedback to delete.")

                delete_id = st.selectbox("Select ID to Delete", fb["id"].tolist())

                if st.button(f"Delete Feedback #{delete_id}", type="primary"):
                    # Delete the specific row
                    exec_sql("DELETE FROM feedback WHERE id = ?;", (int(delete_id),))

                    # Logic to Re-sequence IDs
                    all_data = read_df("SELECT * FROM feedback ORDER BY created_at ASC")
                    if not all_data.empty:
                        # Clear table and reset SQLite auto-increment counter
                        exec_sql("DELETE FROM feedback")
                        exec_sql("DELETE FROM sqlite_sequence WHERE name='feedback'")
                        # Re-insert data to generate new sequential IDs
                        for _, row in all_data.iterrows():
                            exec_sql(
                                "INSERT INTO feedback(created_at, user_name, page, rating, comment) VALUES (?, ?, ?, ?, ?)",
                                (row['created_at'], row['user_name'], row['page'], row['rating'], row['comment'])
                            )
                    else:
                        # If table is empty, just reset the counter
                        exec_sql("DELETE FROM sqlite_sequence WHERE name='feedback'")

                    st.success(f"Feedback deleted and IDs renumbered! ✅")
                    st.rerun()
            else:
                st.warning("Table is empty. Nothing to delete.")


# ==============================
# NAV (Hebrew)
# ==============================

PAGES = {
    "Overview": page_overview,
    "Raw Data Samples": page_raw_data,

    # --- Product & Inventory ---
    "Q1: Pareto (80/20)": page_q1_pareto,
    "Q2: Growth (YoY)": page_q2_perishables,

    # --- Geo & Local Analysis ---
    "Q3: City Preferences": page_q3_city_preferences,
    "Q4: Basket Size": page_q4_basket_size,
    "Q5: Holiday Impact": page_q5_holidays,
    "Q6: Geo Hierarchy": page_q6_cube,

    # --- Macro & Seasonality ---
    "Q7: Seasonality": page_q7_seasonality,
    "Q8: Oil & Economy": page_q8_oil,

    "Feedback": page_feedback,
}


st.sidebar.title("Navigation")
st.sidebar.caption("Dashboard reads from SQLite only (dashboard_gold.db).")
choice = st.sidebar.radio("Go to Page", list(PAGES.keys()))
PAGES[choice]()