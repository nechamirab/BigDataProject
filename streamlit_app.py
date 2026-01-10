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
    page_title="דאשבורד אנליזה – שכבת GOLD",
    layout="wide"
)

# RTL for Hebrew pages (NOT for plots text)
st.markdown(
    """
    <style>
    html, body, [class*="css"]  {
        direction: rtl;
        text-align: right;
    }
    .stSelectbox, .stMultiSelect, .stRadio, .stSlider, .stTextInput, .stTextArea {
        direction: rtl;
        text-align: right;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# ==============================
# DB HELPERS
# ==============================
def _assert_db_exists():
    if not SQLITE_DB_PATH.exists():
        st.error(f"קובץ SQLite לא נמצא: {SQLITE_DB_PATH}")
        st.stop()


def get_conn():
    _assert_db_exists()
    return sqlite3.connect(SQLITE_DB_PATH)


def read_df(query: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql_query(query, conn, params=params)
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
    if df is None or df.empty:
        st.info("אין נתונים להצגה (הטבלה ריקה או שהפילטרים סיננו הכול).")
        return

    styler = df.style
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        styler = styler.background_gradient(subset=num_cols)

    st.dataframe(styler, use_container_width=True, height=height)


def rename_columns_for_display(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = {c: mapping[c] for c in df.columns if c in mapping}
    return df.rename(columns=cols)


def safe_bar_chart(x, y, title, xlabel, ylabel, rotate_xticks=False):
    """Plot with English-only labels to avoid RTL issues."""
    fig = plt.figure()
    plt.bar(x, y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if rotate_xticks:
        plt.xticks(rotation=45, ha="right")
    st.pyplot(fig)


def safe_line_chart(x, y, title, xlabel, ylabel):
    """Plot with English-only labels to avoid RTL issues."""
    fig = plt.figure()
    plt.plot(x, y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    st.pyplot(fig)


def safe_scatter(x, y, title, xlabel, ylabel):
    """Plot with English-only labels to avoid RTL issues."""
    fig = plt.figure()
    plt.scatter(x, y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    st.pyplot(fig)


# ==============================
# LOAD TABLES (cached)
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
# DISPLAY COLUMN MAPPINGS (Hebrew for tables only)
# ==============================
MAP_INVENTORY = {
    "table_name": "שם טבלה",
    "row_count": "מספר שורות",
    "col_count": "מספר עמודות",
}
MAP_Q1 = {
    "item_nbr": "מזהה מוצר",
    "family": "משפחה",
    "total_sales": "סך מכירות",
    "cumulative_pct": "אחוז מצטבר",
    "pareto_group": "קבוצת פארטו",
}
MAP_Q2 = {
    "city": "עיר",
    "family": "משפחה",
    "total_sold": "סך נמכר",
    "rank_in_city": "דירוג בעיר",
}
MAP_Q3 = {
    "city": "עיר",
    "total_items_sold": "סך פריטים שנמכרו",
    "total_transactions": "סך עסקאות",
    "avg_basket_size": "גודל סל ממוצע",
    "city_rank": "דירוג עיר",
}
MAP_Q4 = {
    "city": "עיר",
    "national_holiday_avg": "ממוצע חג לאומי",
    "local_holiday_avg": "ממוצע חג מקומי",
    "winner_type": "מי מנצח",
    "local_holiday_rank": "דירוג (מקומי)",
}
MAP_Q6 = {
    "city": "עיר",
    "sales_year": "שנה",
    "current_sales": "מכירות שנה נוכחית",
    "previous_sales": "מכירות שנה קודמת",
    "growth_pct": "אחוז צמיחה",
}


# ==============================
# PAGES
# ==============================
def page_overview():
    st.title("סקירה כללית")
    explain_box(
        "על מה הדף הזה מדבר?",
        "זהו דף מבוא שמציג תמונת מצב על שכבת ה-GOLD: כמה טבלאות נוצרו, כמה שורות/עמודות יש בכל אחת, "
        "ומה היקף הנתונים הכולל. הדף משתמש בטבלה gold_inventory ומוכיח שהנתונים מוכנים לדאשבורד."
    )
    st.caption(f"מקור נתונים: {SQLITE_DB_PATH}")

    inv = load_inventory()
    if inv is None or inv.empty:
        st.error("הטבלה gold_inventory לא נמצאה או ריקה. ודאי שהרצת את stage_e_analysis.py בהצלחה.")
        return

    gold_count = int((inv["table_name"].astype(str).str.startswith("q")).sum())
    sample_count = int((inv["table_name"].astype(str).str.startswith("sample_")).sum())
    total_rows = int(inv["row_count"].sum())

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("מספר טבלאות GOLD", gold_count)
    with c2:
        st.metric("מספר טבלאות SAMPLE", sample_count)
    with c3:
        st.metric("סך שורות בכל הטבלאות", f"{total_rows:,}")

    st.subheader("מלאי טבלאות (gold_inventory)")
    inv_disp = rename_columns_for_display(inv, MAP_INVENTORY)
    styled_dataframe(inv_disp, height=520)


def page_q1_pareto():
    st.title("שאלה 1: ניתוח פארטו (80/20)")
    explain_box(
        "על מה הדף הזה מדבר?",
        "הדף בודק האם מספר קטן של מוצרים אחראי לרוב המכירות (עקרון 80/20). "
        "מוצגת טבלה של מוצרים לפי סך מכירות, כולל אחוז מצטבר וקבוצת פארטו, ובנוסף גרף שממחיש את עקומת פארטו."
    )

    df = load_q1()
    if df is None or df.empty:
        st.error("q1_pareto_analysis לא קיימת/ריקה ב-SQLite.")
        return

    families = ["(הכול)"] + sorted(df["family"].dropna().unique().tolist())
    selected_family = st.selectbox("סינון לפי משפחת מוצרים", families)

    filtered = df.copy()
    if selected_family != "(הכול)":
        filtered = filtered[filtered["family"] == selected_family]

    st.subheader("תובנה מהירה")
    core_items = (filtered["pareto_group"].astype(str).str.startswith("Top")).sum()
    total_items = len(filtered)
    core_pct = (core_items / total_items) * 100 if total_items else 0
    st.write(f"מתוך **{total_items:,}** מוצרים, כ-**{core_pct:.2f}%** נמצאים בקבוצת 'Top 80% (Core Revenue)'.")
    st.caption("שימי לב: הטקסט בגרפים נשאר באנגלית כדי למנוע בעיות RTL.")

    st.subheader("טבלת מוצרים מובילים (Top 50)")
    disp = rename_columns_for_display(filtered, MAP_Q1)
    styled_dataframe(disp.head(50))

    st.subheader("גרף: Pareto Curve (Top 500)")
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
        "**תיאור הגרף:** ציר ה־X מציג את דירוג המוצרים לפי מכירות (מהמוכר ביותר והלאה), "
        "וציר ה־Y מציג את **אחוז המכירות המצטבר**. "
        "אם העקומה עולה מהר בתחילת הדרך — זה סימן שמעט מוצרים מייצרים חלק גדול מהמכירות."
    )


def page_q2_city_preferences():
    st.title("שאלה 2: העדפות אזוריות לפי עיר (Top-3)")
    explain_box(
        "על מה הדף הזה מדבר?",
        "הדף מציג לכל עיר את 3 משפחות המוצרים הנמכרות ביותר. "
        "אפשר לבחור עיר כדי לראות תוצאות ממוקדות, ולצפות גם בגרף עמודות של שלושת המובילים."
    )

    df = load_q2()
    if df is None or df.empty:
        st.error("q2_top_products_city לא קיימת/ריקה ב-SQLite.")
        return

    cities = ["(הכול)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("בחרי עיר", cities)

    filtered = df.copy()
    if selected_city != "(הכול)":
        filtered = filtered[filtered["city"] == selected_city]

    st.subheader("Top-3 משפחות מוצרים")
    disp = rename_columns_for_display(filtered, MAP_Q2)
    styled_dataframe(disp, height=520)

    if selected_city != "(הכול)" and not filtered.empty:
        st.subheader(f"גרף: Top-3 in {selected_city}")
        safe_bar_chart(
            x=filtered["family"],
            y=filtered["total_sold"],
            title="Top-3 Product Families by City",
            xlabel="Family",
            ylabel="Total Sold",
            rotate_xticks=True
        )
        st.markdown(
            "**תיאור הגרף:** גרף עמודות שמציג את **3 משפחות המוצרים המובילות בעיר שנבחרה**, "
            "כאשר גובה כל עמודה מייצג את **סך היחידות שנמכרו** עבור אותה משפחה. "
            "כך אפשר להבין במה העיר מתמחה מבחינת ביקוש."
        )


def page_q3_basket_size():
    st.title("שאלה 3: גודל סל קנייה ממוצע")
    explain_box(
        "על מה הדף הזה מדבר?",
        "הדף בוחן באילו ערים גודל סל הקנייה הממוצע גדול יותר. "
        "המדד מחושב כ-(סך פריטים שנמכרו) / (סך עסקאות). "
        "אפשר לבחור כמה ערים מובילות להציג (Top-N) ולקבל גם גרף."
    )

    df = load_q3()
    if df is None or df.empty:
        st.error("q3_basket_size_analysis לא קיימת/ריקה ב-SQLite.")
        return

    max_n = max(5, min(22, len(df)))
    top_n = st.slider("כמה ערים להציג (Top-N)", min_value=5, max_value=max_n, value=min(10, max_n), step=1)

    top_df = df.head(top_n).copy()

    st.subheader(f"טבלת Top {top_n} ערים לפי גודל סל")
    disp = rename_columns_for_display(top_df, MAP_Q3)
    styled_dataframe(disp)

    st.subheader("גרף: Basket Size by City (Top-N)")
    safe_bar_chart(
        x=top_df["city"],
        y=top_df["avg_basket_size"],
        title="Average Basket Size by City",
        xlabel="City",
        ylabel="Avg Basket Size",
        rotate_xticks=True
    )
    st.markdown(
        "**תיאור הגרף:** גרף עמודות שמציג לכל עיר (ב־Top-N) את **גודל הסל הממוצע** "
        "(מספר פריטים ממוצע לעסקה). "
        "עמודה גבוהה יותר אומרת שבממוצע לקוחות בעיר קונים יותר פריטים בכל קנייה."
    )


def page_q4_holidays():
    st.title("שאלה 4: השפעת חגים – מקומי מול לאומי")
    explain_box(
        "על מה הדף הזה מדבר?",
        "הדף משווה לכל עיר את ממוצע המכירות בחגים מקומיים מול ממוצע המכירות בחגים לאומיים. "
        "הטבלה מציגה גם מי 'מנצח' בכל עיר, ובגרף פיזור רואים את הפערים בצורה חזותית."
    )

    df = load_q4()
    if df is None or df.empty:
        st.error("q4_holiday_impact לא קיימת/ריקה ב-SQLite.")
        return

    choice = st.radio("סינון לפי 'מי מנצח'", ["הכול", "מקומי", "לאומי"], horizontal=True)
    filtered = df.copy()

    if choice == "מקומי":
        filtered = filtered[filtered["winner_type"] == "Local"]
    elif choice == "לאומי":
        filtered = filtered[filtered["winner_type"] == "National"]

    # display winner in Hebrew (display only)
    filtered_disp = filtered.copy()
    filtered_disp["winner_type"] = filtered_disp["winner_type"].replace({"Local": "מקומי", "National": "לאומי"})

    st.subheader("טבלת השפעת חגים")
    disp = rename_columns_for_display(filtered_disp, MAP_Q4)
    styled_dataframe(disp, height=520)

    st.subheader("גרף: Local vs National (Scatter)")
    safe_scatter(
        x=filtered["national_holiday_avg"],
        y=filtered["local_holiday_avg"],
        title="Local vs National Holiday Impact",
        xlabel="National Holiday Avg Sales",
        ylabel="Local Holiday Avg Sales"
    )
    st.markdown(
        "**תיאור הגרף:** כל נקודה מייצגת עיר. ציר ה־X הוא **ממוצע מכירות בחגים לאומיים**, "
        "וציר ה־Y הוא **ממוצע מכירות בחגים מקומיים**. "
        "נקודות שמופיעות *מעל האלכסון הדמיוני* (Y>X) מעידות שחגים מקומיים חזקים יותר בעיר הזו, "
        "ומתחתיו — חגים לאומיים חזקים יותר."
    )


def page_q5_seasonality():
    st.title("שאלה 5: עונתיות במכירות (PIVOT)")
    explain_box(
        "על מה הדף הזה מדבר?",
        "הדף מציג טבלת PIVOT: שורות הן שנים ועמודות הן חודשים (1–12). "
        "כך אפשר לזהות עונתיות במכירות לאורך השנה. "
        "ניתן לבחור אילו שנים להציג, ובגרף מוצג ממוצע מכירות חודשי על פני השנים שנבחרו."
    )

    df = load_q5()
    if df is None or df.empty:
        st.error("q5_sales_monthly_pivot לא קיימת/ריקה ב-SQLite.")
        return

    years = sorted(df["year"].dropna().unique().tolist())
    selected_years = st.multiselect("בחירת שנים להצגה", years, default=years)
    filtered = df[df["year"].isin(selected_years)].copy()

    st.subheader("טבלת PIVOT (Year × Month)")
    styled_dataframe(filtered, height=380)

    st.subheader("גרף: Monthly Average (selected years)")
    month_cols = [c for c in filtered.columns if str(c) in [str(i) for i in range(1, 13)]]
    if not month_cols:
        st.info("לא נמצאו עמודות חודשים (1..12) בטבלת q5_sales_monthly_pivot.")
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
        "**תיאור הגרף:** קו שמציג את **ממוצע המכירות לכל חודש** (1–12) על פני השנים שנבחרו. "
        "שיאים/שקעים לאורך החודשים מצביעים על עונתיות — חודשים שבהם יש בדרך כלל יותר או פחות מכירות."
    )


def page_q6_perishables():
    st.title("שאלה 6: צמיחה שנתית במוצרים מתכלים (YoY)")
    explain_box(
        "על מה הדף הזה מדבר?",
        "הדף מנתח צמיחה שנה-מול-שנה (YoY) עבור מכירות מוצרים מתכלים. "
        "אפשר לסנן לפי עיר, לראות את אחוז הצמיחה, ולקבל גרף של הערכים הגבוהים ביותר."
    )

    df = load_q6()
    if df is None or df.empty:
        st.error("q6_perishable_growth לא קיימת/ריקה ב-SQLite.")
        return

    cities = ["(הכול)"] + sorted(df["city"].dropna().unique().tolist())
    selected_city = st.selectbox("סינון לפי עיר", cities)

    filtered = df.copy()
    if selected_city != "(הכול)":
        filtered = filtered[filtered["city"] == selected_city]

    st.subheader("טבלת YoY Growth")
    disp = rename_columns_for_display(filtered, MAP_Q6)
    styled_dataframe(disp, height=520)

    st.subheader("גרף: Top 10 by Growth %")
    plot_df = filtered.sort_values("growth_pct", ascending=False).head(10)
    if plot_df.empty:
        st.info("אין נתונים לגרף לפי הפילטר הנוכחי.")
        return

    safe_bar_chart(
        x=plot_df["city"],
        y=plot_df["growth_pct"],
        title="Top 10 Growth Percentage (YoY)",
        xlabel="City",
        ylabel="Growth %"
    )
    st.markdown(
        "**תיאור הגרף:** גרף עמודות שמציג את **10 הערכים הגבוהים ביותר של אחוז הצמיחה (YoY)** "
        "לפי הפילטר הנוכחי. עמודה גבוהה יותר = צמיחה גבוהה יותר לעומת השנה הקודמת. "
        "זה מאפשר לזהות מוקדים שבהם המכירות של מתכלים גדלות מהר."
    )


def page_feedback():
    st.title("משוב משתמשים")
    explain_box(
        "על מה הדף הזה מדבר?",
        "זהו דף אינטראקטיבי שמאפשר להשאיר משוב על הדאשבורד. "
        "המשוב נשמר בטבלת SQLite בשם feedback, ובתחתית הדף מוצגים המשובים האחרונים."
    )
    ensure_feedback_table()

    with st.form("feedback_form"):
        user_name = st.text_input("שם (לא חובה)")
        page = st.selectbox("על איזה דף המשוב?", [
            "סקירה כללית",
            "שאלה 1: פארטו",
            "שאלה 2: העדפות לפי עיר",
            "שאלה 3: גודל סל",
            "שאלה 4: חגים",
            "שאלה 5: עונתיות",
            "שאלה 6: מתכלים",
            "כללי"
        ])
        rating = st.slider("דירוג (1-5)", 1, 5, 5)
        comment = st.text_area("הערות", placeholder="מה עבד טוב? מה לשפר?")

        submitted = st.form_submit_button("שליחה")

    if submitted:
        exec_sql(
            "INSERT INTO feedback(user_name, page, rating, comment) VALUES (?, ?, ?, ?);",
            (user_name, page, int(rating), comment)
        )
        st.success("המשוב נשמר בהצלחה ✅")

    st.subheader("משובים אחרונים")
    fb = read_df("SELECT created_at, user_name, page, rating, comment FROM feedback ORDER BY id DESC LIMIT 20;")
    fb_disp = fb.rename(columns={
        "created_at": "תאריך",
        "user_name": "שם",
        "page": "דף",
        "rating": "דירוג",
        "comment": "תגובה"
    })
    styled_dataframe(fb_disp, height=520)


# ==============================
# NAV (Hebrew)
# ==============================
PAGES = {
    "סקירה כללית": page_overview,
    "שאלה 1: ניתוח פארטו (80/20)": page_q1_pareto,
    "שאלה 2: העדפות לפי עיר (Top-3)": page_q2_city_preferences,
    "שאלה 3: גודל סל קנייה": page_q3_basket_size,
    "שאלה 4: השפעת חגים": page_q4_holidays,
    "שאלה 5: עונתיות במכירות (PIVOT)": page_q5_seasonality,
    "שאלה 6: מוצרים מתכלים (YoY)": page_q6_perishables,
    "משוב": page_feedback,
}

st.sidebar.title("ניווט")
st.sidebar.caption("הדאשבורד קורא מ-SQLite בלבד (dashboard_gold.db).")
choice = st.sidebar.radio("מעבר לדף", list(PAGES.keys()))
PAGES[choice]()
