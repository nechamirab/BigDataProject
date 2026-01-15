import duckdb
import sqlite3
from pathlib import Path

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Paths - Using absolute paths to avoid errors
BASE_DIR = Path(".").resolve()
DUCKLAKE_PATH = BASE_DIR / "full_ducklake" / "my_ducklake.ducklake"
SQLITE_DB_PATH = BASE_DIR / "dashboard_gold.db"  # This is the new "Small/Gold" DB


def init_duckdb_with_sqlite():
    """
    Initializes the DuckDB connection and attaches the SQLite database directly.
    """
    if not DUCKLAKE_PATH.exists():
        raise FileNotFoundError(f"DuckLake not found at: {DUCKLAKE_PATH}")

    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{DUCKLAKE_PATH.as_posix()}' AS my_ducklake;")
    con.execute("USE my_ducklake;")

    # Load the SQLite Extension and Attach the Target DB
    # This enables direct SQL writes from DuckDB to SQLite
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{SQLITE_DB_PATH.as_posix()}' AS gold (TYPE SQLITE);")
    return con

# ==============================================================================
# QUESTION 1: Pareto Analysis (The 80/20 Rule)
# ==============================================================================
def process_question_1(con):
    """
        Q1: Analyze Inventory Efficiency using the Pareto Principle.

        Business Question: Do 20% of our items generate 80% of our revenue?
        Logic: Calculates a running total of sales to identify the revenue cutoff point.
        """
    print("\nProcessing Question 1: Pareto Analysis (Inventory Efficiency)...")

    query = """
    CREATE OR REPLACE TABLE gold.q1_pareto_analysis AS
    WITH item_sales AS (
        -- Step 1: Calculate total sales for each individual product (SKU)
        SELECT 
            i.item_nbr,
            i.family, 
            SUM(t.unit_sales) as total_sales
        FROM train t 
        JOIN items i ON t.item_nbr = i.item_nbr
        GROUP BY 1, 2
    ),
    calc_running_total AS (
        -- Step 2: Calculate Running Total using Window Functions
        SELECT 
            item_nbr,
            family,
            total_sales,
            -- Window function: Sums sales from the best-selling item down to the current row
            SUM(total_sales) OVER (ORDER BY total_sales DESC) as running_total,

            -- Grand total of all sales (required to calculate the percentage share)
            SUM(total_sales) OVER () as grand_total
        FROM item_sales
    )
    -- Step 3: Final Classification into Pareto Categories
    SELECT 
        item_nbr,
        family,
        total_sales,

        -- Calculate cumulative percentage of revenue accumulated so far
        ROUND((running_total / grand_total) * 100, 2) as cumulative_pct,

        -- Business Logic: Tag item as 'Core Revenue' (Top 80%) or 'Dead Stock' (Bottom 20%)
        CASE 
            WHEN (running_total / grand_total) <= 0.80 THEN 'Top 80% (Core Revenue)'
            ELSE 'Bottom 20% (Dead Stock / Long Tail)'
        END as pareto_group

    FROM calc_running_total
    ORDER BY total_sales DESC;
    """

    con.execute(query)
    print("   >> Table 'gold.q1_pareto_analysis' created successfully.")

    # Sanity Check & Insight (Fetch just for display)
    # Preview Top 10
    preview_df = con.execute("SELECT * FROM gold.q1_pareto_analysis LIMIT 10").fetchdf()
    print("   " + "-" * 60)
    print(preview_df.to_string(index=False))
    print("   " + "-" * 60)

    # Calculate Insight Numbers via SQL (Very efficient)
    stats = con.execute("""
            SELECT 
                COUNT(*) as total_items,
                COUNT(CASE WHEN pareto_group LIKE 'Top%' THEN 1 END) as core_items
            FROM gold.q1_pareto_analysis
        """).fetchone()

    total_items = stats[0]
    core_items = stats[1]
    core_pct = (core_items / total_items) * 100

    print(f"   >> INSIGHT: Only {core_pct:.2f}% of items generate 80% of the revenue.")
    if core_pct < 20:
        print("   (Extreme concentration - highly dependent on very few items)")
    elif core_pct > 40:
        print("   (Flatter distribution than typical Pareto)")

# ==============================================================================
# QUESTION 2: Perishable Goods Growth (Year-Over-Year)
# ==============================================================================
def process_question_2(con):
    """
    Q2: Identify top stores for Perishable Goods and calculate Year-Over-Year (YoY) growth.

    Business Question: Which stores handle the most perishable inventory,
    and are they growing or shrinking? (Critical for cold-chain logistics).

    Logic:
    1. Filter for Perishable items only (items.perishable = 1).
    2. Aggregate sales by Store and Year.
    3. Use WINDOW FUNCTION (LAG) to fetch the previous year's sales.
    4. Calculate Growth %: ((Current - Previous) / Previous) * 100.
    """
    print("\nProcessing Question 2: Perishable Goods Growth Analysis...")

    query = """
    CREATE OR REPLACE TABLE gold.q2_perishable_growth AS
    WITH annual_perishable_sales AS (
        -- Step 1: Filter & Aggregate
        -- Get total perishable sales for each store per year
        SELECT 
            s.store_nbr,
            s.city,
            EXTRACT(YEAR FROM t.date) as sales_year,
            SUM(t.unit_sales) as total_perishable_sales
        FROM train t
        JOIN items i ON t.item_nbr = i.item_nbr
        JOIN stores s ON t.store_nbr = s.store_nbr
        WHERE i.perishable = 1  -- Only perishable goods
        GROUP BY s.store_nbr, s.city, sales_year
    ),
    growth_calculation AS (
        -- Step 2: Calculate YoY Growth using LAG
        SELECT 
            store_nbr,
            city,
            sales_year,
            total_perishable_sales,

            -- LAG function: Look back 1 row (within the same store) to get previous year sales
            LAG(total_perishable_sales) OVER (
                PARTITION BY store_nbr 
                ORDER BY sales_year
            ) as prev_year_sales

        FROM annual_perishable_sales
    )
    -- Step 3: Final Calculation & Filtering
    SELECT 
        city,
        sales_year,
        ROUND(total_perishable_sales, 0) as current_sales,
        ROUND(prev_year_sales, 0) as previous_sales,

        -- Calculate Percentage Growth
        ROUND(
            ((total_perishable_sales - prev_year_sales) / prev_year_sales) * 100, 
        2) as growth_pct

    FROM growth_calculation
    WHERE 
        sales_year = 2016
        AND prev_year_sales IS NOT NULL -- Remove first year (no previous data)
    ORDER BY total_perishable_sales DESC -- Show biggest stores first
    LIMIT 20; -- Top 20 stores
    """

    con.execute(query)
    print("   >> Table 'gold.q2_perishable_growth' created successfully.")

    print("   >> Top 20 Growth Stores:")
    df_sample = con.execute("SELECT * FROM gold.q2_perishable_growth LIMIT 10").fetchdf()
    print(df_sample.to_string(index=False))

# ==============================================================================
# QUESTION 3: Regional Preferences (Top-3 Products per City)
# ==============================================================================
def process_question_3(con):
    """
        Q3: Identify top selling product families per city (Localization).

        Business Question: How do consumer preferences vary between cities?
        Logic: Rank product families by sales within each city partition.

        Technical Note:
        We use DuckDB's 'QUALIFY' clause to filter the Top-3 results immediately
        after the window function, without needing a subquery.
        We return the data in 'Long Format' (3 rows per city).
        """
    print("\nProcessing Question 3: Regional Preferences (Top 3)...")

    query = """
    CREATE OR REPLACE TABLE gold.q3_top_products_city AS
    SELECT 
        s.city,
        i.family,
        -- Total sales for this specific city-family combination
        ROUND(SUM(t.unit_sales), 2) as total_sold,
        -- Window Function: Rank families within each city (1 = Best Seller)
        RANK() OVER (PARTITION BY s.city ORDER BY SUM(t.unit_sales) DESC) as rank_in_city
    FROM train t
    JOIN stores s ON t.store_nbr = s.store_nbr
    JOIN items i ON t.item_nbr = i.item_nbr
    GROUP BY s.city, i.family
    -- DuckDB Exclusive: Filter window results directly (No subquery needed!)
    QUALIFY rank_in_city <= 3
    ORDER BY s.city, rank_in_city;
    """

    con.execute(query)
    print("   >> Table 'gold.q3_top_products_city' created successfully.")

    # Print Sample
    print("   >> Top 3 Preferences (Sample):")
    df_sample = con.execute("SELECT * FROM gold.q3_top_products_city LIMIT 6").fetchdf()
    print(df_sample.to_string(index=False))


# ==============================================================================
# QUESTION 4: Basket Size Analysis
# ==============================================================================
def process_question_4(con):
    """
    Q4: Analyze 'Basket Size' efficiency per city.

    Business Question: Which cities have the largest average transaction size?
    Logic: (Total Items Sold) / (Total Transactions).

    Technical Note:
    We use a CTE (Pre-Aggregation) to aggregate sales data per store/date
    BEFORE joining with transactions. This prevents 'Fan-out' (duplication)
    of the transaction counts, which would lead to incorrect averages.
    """
    print("\nProcessing Question 4: Basket Size Analysis by City...")

    query = """
    CREATE OR REPLACE TABLE gold.q4_basket_size_analysis AS
    WITH daily_sales_agg AS (
        -- Step 1: Pre-aggregate items sold per store per day.
        -- This collapses the 'train' table (millions of rows) into one row per store/date.
        SELECT 
            store_nbr,
            date,
            SUM(unit_sales) as daily_items
        FROM train
        GROUP BY store_nbr, date
    ),
    city_stats AS (
        -- Step 2: Join the aggregated sales with transactions and store metadata.
        -- Now the join is 1:1 (one row per day), so transaction counts are accurate.
        SELECT 
            s.city,
            SUM(dsa.daily_items) as total_items_sold,
            SUM(tr.transactions) as total_transactions
        FROM daily_sales_agg dsa
        JOIN transactions tr 
            ON dsa.store_nbr = tr.store_nbr AND dsa.date = tr.date
        JOIN stores s 
            ON dsa.store_nbr = s.store_nbr
        GROUP BY s.city
    )
    SELECT 
        city,
        total_items_sold,
        total_transactions,

        -- Calculate the Average Basket Size (Items per Transaction)
        ROUND(total_items_sold / total_transactions, 2) as avg_basket_size,

        -- Window Function Requirement: 
        -- Rank cities based on basket size (1 = Largest basket)
        DENSE_RANK() OVER (ORDER BY (total_items_sold / total_transactions) DESC) as city_rank
    FROM city_stats
    ORDER BY city_rank;
    """

    con.execute(query)
    print("   >> Table 'gold.q4_basket_size_analysis' created successfully.")

    # Print Sample
    print("   >> Top 5 Cities by Basket Size:")
    df_sample = con.execute("SELECT * FROM gold.q4_basket_size_analysis LIMIT 5").fetchdf()
    print(df_sample.to_string(index=False))

# ==============================================================================
# QUESTION 5: Local vs. National Holidays Impact
# ==============================================================================
def process_question_5(con):
    """
    Q5: Analyze the impact of 'Local' vs 'National' holidays.
    UPDATED: Now includes a 'winner_type' column to explicitly show
    which holiday type generates more sales.
    """
    print("\nProcessing Question 5: Local vs. National Holidays...")

    query = """
    CREATE OR REPLACE TABLE gold.q5_holiday_impact AS
    WITH city_daily_sales AS (
        -- Step 1: Sales per City per Date
        SELECT 
            s.city,
            t.date,
            SUM(t.unit_sales) as total_daily_sales
        FROM train t
        JOIN stores s ON t.store_nbr = s.store_nbr
        GROUP BY s.city, t.date
    ),
    holiday_stats_raw AS (
        -- Step 2: Join with Holidays and Pivot Data
        -- We calculate the averages here first
        SELECT 
            cs.city,
            -- Calculate National Average
            MAX(CASE WHEN h.locale = 'National' THEN cs.total_daily_sales END) as national_avg_raw,
            -- Calculate Local Average (Only if locale matches city)
            MAX(CASE WHEN h.locale = 'Local' AND h.locale_name = cs.city THEN cs.total_daily_sales END) as local_avg_raw
        FROM city_daily_sales cs
        JOIN holidays_events h ON cs.date = h.date
        WHERE 
            h.type = 'Holiday' 
            AND h.transferred = 'false'
        GROUP BY cs.city, h.locale -- Group by locale to separate the data first
    ),
    -- Step 3: Clean Pivot (Combine rows back to one per city)
    final_averages AS (
        SELECT
            city,
            AVG(national_avg_raw) as national_holiday_avg,
            AVG(local_avg_raw) as local_holiday_avg
        FROM holiday_stats_raw
        GROUP BY city
        HAVING local_holiday_avg IS NOT NULL -- Keep only relevant cities
    )
    -- Step 4: Final Selection + Comparison Column 
    SELECT 
        city,
        ROUND(national_holiday_avg, 2) as national_holiday_avg,
        ROUND(local_holiday_avg, 2) as local_holiday_avg,

        CASE 
            WHEN local_holiday_avg > national_holiday_avg THEN 'Local'
            ELSE 'National'
        END as winner_type,

        -- Window Function: Rank by Local Holiday Sales
        RANK() OVER (ORDER BY local_holiday_avg DESC) as local_holiday_rank
    FROM final_averages
    ORDER BY local_holiday_rank;
    """

    con.execute(query)
    print("   >> Table 'gold.q5_holiday_impact' created successfully.")

    print("   >> Holiday Impact Sample:")
    df_sample = con.execute("SELECT * FROM gold.q5_holiday_impact LIMIT 5").fetchdf()
    print(df_sample.to_string(index=False))


def process_question_6_cube(con):
    """
    Q6: Geographic Hierarchy using CUBE.
    Analyzes sales at multiple levels: National -> State -> City.
    CUBE generates subtotals for all combinations.
    """
    print("\nProcessing Question 6: Geographic Hierarchy (CUBE)...")

    query = """
    CREATE OR REPLACE TABLE gold.q6_geo_cube AS
    SELECT 
        -- COALESCE is used because CUBE produces NULLs for the subtotals
        COALESCE(s.state, 'Grand Total') as state,
        COALESCE(s.city, 'All Cities') as city,
        ROUND(SUM(t.unit_sales), 2) as total_sales,

        -- Tagging the level of aggregation for easier filtering in the dashboard
        CASE 
            WHEN s.state IS NULL AND s.city IS NULL THEN 'National Level'
            WHEN s.state IS NOT NULL AND s.city IS NULL THEN 'State Level'
            ELSE 'City Level'
        END as aggregation_level

    FROM train t
    JOIN stores s ON t.store_nbr = s.store_nbr
    GROUP BY CUBE(s.state, s.city)
    ORDER BY total_sales DESC;
    """

    con.execute(query)
    print("   >> Table 'gold.q6_geo_cube' created successfully.")

    df = con.execute("SELECT * FROM gold.q6_geo_cube LIMIT 10").fetchdf()
    print(df.to_string(index=False))


def process_question_7_pivot(con):
    """
    Q7: Seasonality (PIVOT) - monthly total sales per year.
    Business Question: Is there seasonality? Which months are strongest each year?
    Requirement: Uses DuckDB PIVOT and saves the output into SQLite (gold).
    """
    print("\nProcessing Question 7: Seasonality Pivot (Monthly Sales by Year)...")

    query = """
    CREATE OR REPLACE TABLE gold.q7_sales_monthly_pivot AS
    WITH sales_prep AS (
        SELECT
            EXTRACT(YEAR FROM date) AS year,
            EXTRACT(MONTH FROM date) AS month,
            unit_sales
        FROM train
    )
    PIVOT sales_prep
    ON month IN (1,2,3,4,5,6,7,8,9,10,11,12)
    USING ROUND(SUM(unit_sales), 2)
    GROUP BY year
    ORDER BY year;
    """
    con.execute(query)
    print("   >> Table 'gold.q7_sales_monthly_pivot' created successfully.")

    df_sample = con.execute("SELECT * FROM gold.q7_sales_monthly_pivot LIMIT 5").fetchdf()
    print(df_sample.to_string(index=False))


def process_question_8_oil(con):
    """
    Q8: Macro-Economic Analysis (Oil vs Sales).
    Merges external oil data with internal sales data to find correlations.
    Aggregates by MONTH to smooth out daily volatility.
    """
    print("\nProcessing Question 8: Oil Price Impact...")

    query = """
    CREATE OR REPLACE TABLE gold.q8_oil_sales AS
    WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', date) as m_date,
            SUM(unit_sales) as total_sales
        FROM train
        GROUP BY 1
    ),
    monthly_oil AS (
        SELECT 
            DATE_TRUNC('month', date) as m_date,
            AVG(dcoilwtico) as avg_oil_price
        FROM oil
        GROUP BY 1
    )
    SELECT 
        s.m_date as month,
        s.total_sales,
        o.avg_oil_price
    FROM monthly_sales s
    JOIN monthly_oil o ON s.m_date = o.m_date
    ORDER BY s.m_date;
    """

    con.execute(query)
    print("   >> Table 'gold.q8_oil_sales' created successfully.")
    df = con.execute("SELECT * FROM gold.q8_oil_sales LIMIT 10").fetchdf()
    print(df.to_string(index=False))
# =============================================

def save_raw_samples(con):
    print("\n--- Saving Raw Samples to SQLite ---")

    tables_to_sample = ['train', 'items', 'stores', 'transactions', 'oil', 'holidays_events']

    for tbl in tables_to_sample:
        con.execute(f"""
                    CREATE OR REPLACE TABLE gold.sample_{tbl} AS 
                    SELECT * FROM {tbl} USING SAMPLE 300
                """)
        print(f"   >> Created 'gold.sample_{tbl}' successfully.")


def vacuum_sqlite_database():
    """
    Final Cleanup: Runs the VACUUM command on the SQLite database.
    This rebuilds the database file, removing any 'dead' space from deleted rows
    and ensuring the file size is as small as possible for submission.
    """
    print("\n--- Final Step: Optimizing SQLite File Size (VACUUM) ---")

    # אנחנו מתחברים ישירות לקובץ ה-SQLite לרגע אחד כדי לנקות אותו
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)

        # הרצת פקודת הניקיון
        conn.execute("VACUUM;")

        conn.close()
        print("   >> VACUUM completed successfully. File is clean and compact.")
    except Exception as e:
        print(f"   >> Warning: Could not run VACUUM: {e}")

def verify_sqlite_tables_direct():
    print("\n--- Direct SQLite Verification ---")
    conn = sqlite3.connect(SQLITE_DB_PATH)
    rows = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        ORDER BY name
    """).fetchall()
    conn.close()

    if not rows:
        print("SQLite file contains NO tables.")
    else:
        print("SQLite tables:")
        for (name,) in rows:
            print(" -", name)

def print_gold_catalog(con, preview_rows: int = 5):
    """
    Prints all user-created GOLD tables in the attached SQLite DB, with:
    - row count
    - column count
    - preview rows

    Notes:
    - In your environment, DuckDB suggests "main.sqlite_master" (not gold.main.sqlite_master).
    - We skip system/metadata tables (sqlite_*, ducklake_*, __*).
    """
    print("\n" + "=" * 70)
    print("--- GOLD (SQLite) CATALOG ---")
    print("=" * 70)

    # 1) Show attached DBs (sanity check)
    try:
        attached = con.execute("PRAGMA database_list;").fetchdf()
        print("\nAttached DBs (DuckDB view):")
        print(attached.to_string(index=False))
    except Exception as e:
        print(f"Could not read PRAGMA database_list: {e}")

    # 2) List tables from SQLite's sqlite_master (reliable)
    try:
        tables = con.execute("""
            SELECT name
            FROM main.sqlite_master
            WHERE type='table'
            ORDER BY name
        """).fetchall()
    except Exception as e:
        print(f"\nERROR: Could not query main.sqlite_master: {e}")
        return

    if not tables:
        print("\nNo tables found in SQLite (main.sqlite_master).")
        return

    # 3) Iterate and print metadata + preview
    for (tname,) in tables:
        # Keep only our project tables inside SQLite
        if not (tname.startswith("q") or tname.startswith("sample_") or tname in ("gold_inventory", "feedback")):
            continue

        try:
            # Row count
            row_cnt = con.execute(f"SELECT COUNT(*) FROM gold.{tname}").fetchone()[0]

            # Column count
            col_cnt = con.execute(
                f"SELECT COUNT(*) FROM pragma_table_info('gold.{tname}')"
            ).fetchone()[0]

            print(f"\nTable: gold.{tname} | rows={row_cnt:,} | cols={col_cnt}")

            # Preview
            df = con.execute(f"SELECT * FROM gold.{tname} LIMIT {preview_rows}").fetchdf()
            print(df.to_string(index=False))

        except Exception as e:
            print(f"\nTable: gold.{tname} (skipped) | reason: {e}")

def create_gold_inventory(con):
    """
    Creates a GOLD inventory table in SQLite:
    table_name | row_count | col_count
    """
    print("\n--- Creating GOLD inventory table ---")

    # Create empty table
    con.execute("""
        CREATE OR REPLACE TABLE gold.gold_inventory AS
        SELECT 
            ''::VARCHAR AS table_name,
            0::BIGINT AS row_count,
            0::BIGINT AS col_count
        WHERE FALSE;
    """)

    # Read SQLite catalog
    tables = con.execute("""
        SELECT name
        FROM main.sqlite_master
        WHERE type='table'
        ORDER BY name
    """).fetchall()

    for (tname,) in tables:
        # Only our project tables
        if not (tname.startswith("q") or tname.startswith("sample_")):
            continue

        row_cnt = con.execute(f"SELECT COUNT(*) FROM gold.{tname}").fetchone()[0]
        col_cnt = con.execute(
            f"SELECT COUNT(*) FROM pragma_table_info('gold.{tname}')"
        ).fetchone()[0]

        con.execute(
            "INSERT INTO gold.gold_inventory VALUES (?, ?, ?);",
            [tname, row_cnt, col_cnt]
        )

    df = con.execute("""
        SELECT * FROM gold.gold_inventory
        ORDER BY table_name
    """).fetchdf()

    print(df.to_string(index=False))

# ==============================================================================
# MAIN EXECUTION BLOCK
# ==============================================================================
if __name__ == "__main__":
    con = None
    try:
        # Initialize Connection (Attach both DBs)
        con = init_duckdb_with_sqlite()

        process_question_1(con)
        process_question_2(con)
        process_question_3(con)
        process_question_4(con)
        process_question_5(con)
        process_question_6_cube(con)
        process_question_7_pivot(con)
        process_question_8_oil(con)
        save_raw_samples(con)
        create_gold_inventory(con)

        verify_sqlite_tables_direct()
        print_gold_catalog(con, preview_rows=5)

    except Exception as e:
        print(f"Error during execution: {e}")

    finally:
        # Always close connections
        if con:
            con.close()

        vacuum_sqlite_database()
        print("\nDone.")