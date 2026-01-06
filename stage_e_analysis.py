import duckdb
import sqlite3
import pandas as pd
from pathlib import Path

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Paths - Using absolute paths to avoid errors
BASE_DIR = Path(".").resolve()
DUCKLAKE_PATH = BASE_DIR / "full_ducklake" / "my_ducklake.ducklake"
SQLITE_DB_PATH = BASE_DIR / "dashboard_gold.db"  # This is the new "Small/Gold" DB


def get_duckdb_con():
    """Connects to the Big Data Lake (DuckDB)"""
    if not DUCKLAKE_PATH.exists():
        raise FileNotFoundError(f"DuckLake not found at: {DUCKLAKE_PATH}")

    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{DUCKLAKE_PATH.as_posix()}' AS my_ducklake;")
    con.execute("USE my_ducklake;")
    return con


def get_sqlite_con():
    """Connects to the Small Gold Database (SQLite)"""
    # SQLite will create the file if it doesn't exist
    return sqlite3.connect(SQLITE_DB_PATH)


# ==============================================================================
# QUESTION 1: Basket Size Analysis
# ==============================================================================
def process_question_1(duck_con, sqlite_con):
    """
    Q1: Analyze 'Basket Size' efficiency per city.

    Business Question: Which cities have the largest average transaction size?
    Logic: (Total Items Sold) / (Total Transactions).

    Technical Note:
    We use a CTE (Pre-Aggregation) to aggregate sales data per store/date
    BEFORE joining with transactions. This prevents 'Fan-out' (duplication)
    of the transaction counts, which would lead to incorrect averages.
    """
    print("\nProcessing Question 1: Basket Size Analysis by City...")

    query = """
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

    # 1. Execute query in DuckDB (High Performance Engine)
    df = duck_con.execute(query).fetchdf()

    # 2. Sanity Check: Ensure results are logical
    if not df.empty:
        print("\n   >> Top 5 Cities by Basket Size:")
        print("   " + "-" * 60)
        # Prints the first 5 lines
        print(df.head(5).to_string(index=False))
        print("   " + "-" * 60)

        top_val = df.iloc[0]['avg_basket_size']
        if top_val < 1:
            print("   WARNING: Basket size is suspiciously low (<1). Check Logic!")
        else:
            print(f"   >> Data looks logical (Top size: {top_val}).")

    # 3. Save the result to SQLite (Gold Layer for Dashboard)
    table_name = "q1_basket_size_analysis"
    df.to_sql(table_name, sqlite_con, if_exists="replace", index=False)
    print(f"   >> Saved result to SQLite table: '{table_name}'")


# ==============================================================================
# QUESTION 2: Local vs. National Holidays Impact
# ==============================================================================
def process_question_2(duck_con, sqlite_con):
    """
    Q2: Analyze the impact of 'Local' vs 'National' holidays.
    UPDATED: Now includes a 'winner_type' column to explicitly show
    which holiday type generates more sales.
    """
    print("\nProcessing Question 2: Local vs. National Holidays...")

    query = """
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

    # 1. Execute in DuckDB
    df = duck_con.execute(query).fetchdf()

    # 2. Print Sample
    if not df.empty:
        print("\n   >> Top 5 Cities - Holiday Impact Comparison:")
        print("   " + "-" * 90)
        print(df.head(5).to_string(index=False))
        print("   " + "-" * 90)

    # 3. Save to SQLite
    table_name = "q2_holiday_impact"
    df.to_sql(table_name, sqlite_con, if_exists="replace", index=False)
    print(f"\n   >> Saved result to SQLite table: '{table_name}'")


# ==============================================================================
# QUESTION 3: Perishable Goods Growth (Year-Over-Year)
# ==============================================================================
def process_question_3(duck_con, sqlite_con):
    """
    Q3: Identify top stores for Perishable Goods and calculate Year-Over-Year (YoY) growth.

    Business Question: Which stores handle the most perishable inventory,
    and are they growing or shrinking? (Critical for cold-chain logistics).

    Logic:
    1. Filter for Perishable items only (items.perishable = 1).
    2. Aggregate sales by Store and Year.
    3. Use WINDOW FUNCTION (LAG) to fetch the previous year's sales.
    4. Calculate Growth %: ((Current - Previous) / Previous) * 100.
    """
    print("\nProcessing Question 3: Perishable Goods Growth Analysis...")

    query = """
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
    LIMIT 10; -- Top 10 stores
    """

    # 1. Execute in DuckDB
    df = duck_con.execute(query).fetchdf()

    # 2. Print Sample
    if not df.empty:
        print("\n   >> Top 10 Stores - Perishable Sales & Growth (2016):")
        print("   " + "-" * 90)
        print(df.to_string(index=False))
        print("   " + "-" * 90)

    # 3. Save to SQLite
    table_name = "q3_perishable_growth"
    df.to_sql(table_name, sqlite_con, if_exists="replace", index=False)
    print(f"\n   >> Saved result to SQLite table: '{table_name}'")

# ==============================================================================
# MAIN EXECUTION BLOCK
# ==============================================================================
if __name__ == "__main__":
    # Initialize connections
    duck_con = get_duckdb_con()
    sqlite_con = get_sqlite_con()

    try:
        # Run Analysis for Question 1
        process_question_1(duck_con, sqlite_con)
        # Run Analysis for Question 2
        process_question_2(duck_con, sqlite_con)
        # Run Analysis for Question 3
        process_question_3(duck_con, sqlite_con)

    except Exception as e:
        print(f"Error during execution: {e}")

    finally:
        # Always close connections
        duck_con.close()
        sqlite_con.close()
        print("\nDone.")