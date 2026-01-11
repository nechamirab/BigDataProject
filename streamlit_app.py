import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

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
    """Top-of-page explanation box (Hebrew)."""
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
    """Plot with English-only labels to avoid RTL issues."""
    fig = plt.figure(figsize=(10, 5))
    plt.bar(x, y, color='pink')
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    if rotate_xticks:
        plt.xticks(rotation=45, ha="right")
    st.pyplot(fig)


def safe_line_chart(x, y, title, xlabel, ylabel):
    """Plot with English-only labels to avoid RTL issues."""
    fig = plt.figure(figsize=(10, 5))
    plt.plot(x, y, marker='o', linestyle='-', color='purple')
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    st.pyplot(fig)


def safe_scatter(x, y, title, xlabel, ylabel):
    """Plot with English-only labels to avoid RTL issues."""
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
    "family": "Category",
    "total_sold": "Total sold",
    "rank_in_city": "Rank in city",
}
MAP_Q3 = {
    "city": "City",
    "total_items_sold": "Total items sold",
    "total_transactions": "Total transactions",
    "avg_basket_size": "Average basket size",
    "city_rank": "City rank",
}
MAP_Q4 = {
    "city": "City",
    "national_holiday_avg": "National holiday average",
    "local_holiday_avg": "Local holiday average",
    "winner_type": "The winner",
    "local_holiday_rank": "Local holiday rank",
}
MAP_Q6 = {
    "city": "City",
    "sales_year": "Year",
    "current_sales": "Current year sales",
    "previous_sales": "Previous year sales",
    "growth_pct": "percent growth",
}


# ==============================
# PAGES
# ==============================
def page_overview():
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
    NEW PAGE: Displays raw data samples (required by assignment).
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

def page_q2_city_preferences():
    st.title("Q2: Regional Preferences (Top-3) ⛰️")
    explain_box(
        "Localization Analysis",
        "What are the top 3 selling product categories in each city? This helps understand local consumer behavior.\n\n"
             "**What's on this page:** A city selector, a bar chart displaying the top 3 categories for the selected city, and a detailed table of rankings."
    )

    df = load_q2()
    if df is None or df.empty:
        st.error("q2_top_products_city does not exist/is empty in SQLite.")
        return

    cities = ["(All)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Select City", cities)

    filtered = df.copy()
    if selected_city != "(All)":
        filtered = filtered[filtered["city"] == selected_city]

    st.subheader(f"Top Categories in {selected_city}")
    disp = rename_columns_for_display(filtered, MAP_Q2)
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


def page_q3_basket_size():
    st.title("Q3: Average Basket Size 🛒")
    explain_box(
        "Store Efficiency",
        "Which cities have the largest average basket size (items per transaction)?\n\n"
        "**What's on this page:** A slider to choose the number of top cities, a bar chart comparing basket sizes, and a ranked table."
    )

    df = load_q3()
    if df is None or df.empty:
        st.error("q3_basket_size_analysis does not exist/is empty in SQLite.")
        return

    max_n = max(5, min(22, len(df)))
    top_n = st.slider("Select Top N Cities", min_value=5, max_value=max_n, value=min(10, max_n), step=1)

    top_df = df.head(top_n).copy()

    st.subheader(f"Top {top_n} Cities by Basket Size")
    disp = rename_columns_for_display(top_df, MAP_Q3)
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


def page_q4_holidays():
    st.title("Q4: Local vs. National Holidays 🎊")
    explain_box(
        "Holiday Impact Analysis",
        "Do local holidays generate more sales than national holidays in specific cities?\n\n"
        "**What's on this page:** A scatter plot where each point represents a city, and a comparison table. Points above the diagonal line indicate stronger Local holidays."
    )

    df = load_q4()
    if df is None or df.empty:
        st.error("q4_holiday_impact does not exist/is empty in SQLite.")
        return

    choice = st.radio("Filter by Winner", ["All", "Local", "National"], horizontal=True)
    filtered = df.copy()

    if choice == "Local":
        filtered = filtered[filtered["winner_type"] == "Local"]
    elif choice == "National":
        filtered = filtered[filtered["winner_type"] == "National"]

    st.subheader("Holiday Impact Table")
    disp = rename_columns_for_display(filtered, MAP_Q4)
    styled_dataframe(disp, height=520)

    st.subheader("Chart: Local vs National (Scatter)")

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.scatterplot(
        data=filtered,
        x='national_holiday_avg',
        y='local_holiday_avg',
        hue='winner_type',
        style='winner_type',
        s=100,
        ax=ax
    )

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),
        np.max([ax.get_xlim(), ax.get_ylim()]),
    ]
    ax.plot(lims, lims, 'r--', alpha=0.5, zorder=0)
    ax.set_title("Local vs National Holiday Impact", fontsize=16)
    ax.set_xlabel("National Holiday Avg Sales", fontsize=12)
    ax.set_ylabel("Local Holiday Avg Sales", fontsize=12)
    st.pyplot(fig)
    st.markdown(
        "**Chart Description:** Each point represents a city. The X-axis is **Average National Holiday Sales**, "
        "and the Y-axis is **Average Local Holiday Sales**. "
        "Points appearing *above the diagonal line* (Y>X) indicate that Local holidays generate more sales in that city."
    )


def page_q5_seasonality():
    st.title("Q5: Seasonality (Pivot) 📈")
    explain_box(
        "Monthly Sales Trends",
        "This page analyzes sales trends over time, aggregated by month and year, to identify seasonal peaks.\n\n"
        "**What's on this page:** A filter for years, a line chart showing monthly sales, and a Pivot table."
    )

    df = load_q5()
    if df is None or df.empty:
        st.error("q5_sales_monthly_pivot does not exist/is empty in SQLite.")
        return

    years = sorted(df["year"].dropna().unique().tolist())
    selected_years = st.multiselect("Select Years", years, default=years)
    filtered = df[df["year"].isin(selected_years)].copy()

    st.subheader("Pivot Table (Year × Month)")
    styled_dataframe(filtered, height=380)

    st.subheader("Chart: Monthly Average (selected years)")
    month_cols = [c for c in filtered.columns if str(c) in [str(i) for i in range(1, 13)]]
    if not month_cols:
        st.info("No month columns (1..12) found in table q5_sales_monthly_pivot.")
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


def page_q6_perishables():
    st.title("Q6: Perishable Goods Growth (YoY) ♻️")
    explain_box(
        "Year-Over-Year (YoY) Growth",
        "Which cities are showing the highest growth in the sales of perishable goods?\n\n"
        "**What's on this page:** A bar chart showing the top 20 cities by growth percentage, and a comparison table."
    )

    df = load_q6()
    if df is None or df.empty:
        st.error("q6_perishable_growth does not exist/is empty in SQLite.")
        return

    cities = ["(All)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Filter by City", cities)

    filtered = df.copy()
    if selected_city != "(All)":
        filtered = filtered[filtered["city"] == selected_city]

    st.subheader("YoY Growth Table")
    disp = rename_columns_for_display(filtered, MAP_Q6)
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


def page_feedback():
    st.title("User Feedback 💬")
    explain_box(
        "We value your feedback",
        "Please rate the dashboard and provide your comments. Feedback is saved to the SQLite database."
    )
    ensure_feedback_table()

    ADMIN_PASSWORD = "12345"

    # --- ניהול מצב התחברות ---
    if "admin_logged_in" not in st.session_state:
        st.session_state["admin_logged_in"] = False

    # --- חלק 1: הוספת משוב ---
    st.subheader("Add New Feedback")
    with st.form("feedback_form", clear_on_submit=True):
        user_name = st.text_input("Name (Optional)")
        page = st.selectbox("Which page are you reviewing?", [
            "Overview",
            "Raw Data Samples",
            "Q1: Pareto (80/20)",
            "Q2: City Preferences",
            "Q3: Basket Size",
            "Q4: Holiday Impact",
            "Q5: Seasonality",
            "Q6: Growth (YoY)",
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

    # --- חלק 2: צפייה במשובים ---
    st.subheader("Recent Feedback")
    fb = read_df("SELECT id, created_at, user_name, page, rating, comment FROM feedback ORDER BY id DESC;")

    # הצגת הטבלה (רק אם יש נתונים)
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

    # --- חלק 3: אזור מנהל ---
    st.markdown("---")
    with st.expander("🔒 Admin Area"):

        # מצב א': לא מחובר
        if not st.session_state["admin_logged_in"]:
            password_input = st.text_input("Enter Admin Password:", type="password")
            if st.button("Login"):
                if password_input == ADMIN_PASSWORD:
                    st.session_state["admin_logged_in"] = True
                    st.success("Logged in successfully!")
                    st.rerun()
                else:
                    st.error("Incorrect Password ❌")

        # מצב ב': מחובר
        else:
            # כפתור התנתקות
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
                    # 1. מחיקה
                    exec_sql("DELETE FROM feedback WHERE id = ?;", (int(delete_id),))

                    # 2. סידור מחדש אוטומטי
                    all_data = read_df("SELECT * FROM feedback ORDER BY created_at ASC")
                    if not all_data.empty:
                        # אם נשארו רשומות - מסדרים מחדש
                        exec_sql("DELETE FROM feedback")
                        exec_sql("DELETE FROM sqlite_sequence WHERE name='feedback'")
                        for _, row in all_data.iterrows():
                            exec_sql(
                                "INSERT INTO feedback(created_at, user_name, page, rating, comment) VALUES (?, ?, ?, ?, ?)",
                                (row['created_at'], row['user_name'], row['page'], row['rating'], row['comment'])
                            )
                    else:
                        # אם מחקנו את האחרון - רק מאפסים את המונה
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
    "Q1: Pareto (80/20)": page_q1_pareto,
    "Q2: City Preferences": page_q2_city_preferences,
    "Q3: Basket Size": page_q3_basket_size,
    "Q4: Holiday Impact": page_q4_holidays,
    "Q5: Seasonality": page_q5_seasonality,
    "Q6: Growth (YoY)": page_q6_perishables,
    "Feedback": page_feedback,
}

st.sidebar.title("Navigation")
st.sidebar.caption("Dashboard reads from SQLite only (dashboard_gold.db).")
choice = st.sidebar.radio("Go to Page", list(PAGES.keys()))
PAGES[choice]()