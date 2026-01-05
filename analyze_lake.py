import duckdb
from pathlib import Path

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# True = Connect to the Full Lake
# False = Connect to the Sample Lake
USE_FULL_LAKE = True

# Define paths
BASE_DIR = Path(".")
if USE_FULL_LAKE:
    LAKE_DIR = BASE_DIR / "full_ducklake"
    MODE_NAME = "FULL Data Lake"
else:
    LAKE_DIR = BASE_DIR / "submission_ducklake"
    MODE_NAME = "SAMPLE Data Lake"

LAKE_METADATA_FILE = LAKE_DIR / "my_ducklake.ducklake"
LAKE_FILES_DIR = LAKE_DIR / "my_ducklake.ducklake.files"

# ==============================================================================
# UI / PRINTING HELPERS
# ==============================================================================
def print_header(title):
    """Prints a main section header with double lines."""
    print(f"\n{'=' * 60}")
    print(f"--- {title} ---")
    print(f"{'=' * 60}")

def print_subsection(title):
    """Prints a sub-section header with a single underline."""
    print(f"\n{title}")
    print(f"{'-' * 40}")

def format_bytes(size):
    """Formats bytes into readable strings (KB, MB, GB)."""
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"

# ==============================================================================
# CORE FUNCTIONS
# ==============================================================================
def get_lake_connection() -> duckdb.DuckDBPyConnection:
    """
    Connects to the EXISTING DuckLake.
    """
    if not LAKE_METADATA_FILE.exists():
        raise FileNotFoundError(f"Could not find lake file at: {LAKE_METADATA_FILE}")
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    print_subsection(f"Connecting to {MODE_NAME}")
    print(f"Path: {LAKE_METADATA_FILE}")
    con.execute(f"ATTACH 'ducklake:{LAKE_METADATA_FILE.as_posix()}' AS my_ducklake;")
    con.execute("USE my_ducklake;")
    return con

def analyze_table_count(con):
    print_header("1. Number of Tables")
    result = con.execute("SELECT COUNT(*) FROM (SHOW TABLES)").fetchone()[0]
    print(f"Total Tables: {result}")


def analyze_row_counts(con, tables):
    print_header("2. Row Counts per Table")
    print(f"{'Table Name':<20} | {'Row Count':<15}")
    print("-" * 40)

    total_rows = 0
    for table in tables:
        table_name = table[0]
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"{table_name:<20} | {count:<15,}")
        total_rows += count

    print("-" * 40)
    print(f"Total Rows in Lake: {total_rows:,}")


def analyze_nulls(con, tables):
    print_header("3. NULL Counts per Column")

    for table in tables:
        table_name = table[0]
        print_subsection(f"Table: {table_name}")

        columns_info = con.execute(f"DESCRIBE {table_name}").fetchall()
        print(f"   {'Column':<20} | {'Nulls':<10} | {'%'}")

        total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        for col in columns_info:
            col_name = col[0]
            null_count = con.execute(f"SELECT COUNT(*) - COUNT({col_name}) FROM {table_name}").fetchone()[0]
            percent = (null_count / total_rows * 100) if total_rows > 0 else 0
            print(f"   {col_name:<20} | {null_count:<10,} | {percent:.2f}%")

def analyze_files(con):
    print_header("4. Physical Storage Analysis")

    # File Types
    path_pattern = f"{LAKE_FILES_DIR.as_posix()}/**/*"

    types_query = f"""
            SELECT '.' || list_last(string_split(file, '.')) as ext, COUNT(*) as cnt
            FROM glob('{path_pattern}')
            GROUP BY ext
            ORDER BY cnt DESC
    """
    print(f"{'File Type':<15} | {'Count':<10}")
    print("-" * 30)

    total_files = 0

    try:
        for ext, count in con.execute(types_query).fetchall():
            display = ext if len(ext) < 10 else "Other"
            print(f"{display:<15} | {count:<10,}")
            total_files += count
        print("-" * 30)
        print(f"Total Files:    {total_files:,}")
    except Exception as e:
        print(f"Error: {e}")

    # Volume
    print("\nCalculating Volume (Parquet)...")
    try:
        parquet_pattern = f"{LAKE_FILES_DIR.as_posix()}/**/*.parquet"
        size_bytes = con.execute(
            f"SELECT COALESCE(SUM(total_compressed_size), 0) FROM parquet_metadata('{parquet_pattern}')").fetchone()[
            0]
        print(f"Total Data Size: {format_bytes(size_bytes)}")
    except:
        print("Warning: Could not calc size.")

def analyze_column_counts(con, tables):
    """ Query 5: How many columns are in each table? """
    print("\n--- 5. Column Counts per Table ---")

    print(f"{'Table Name':<20} | {'Column Count':<15}")
    print("-" * 40)

    for table in tables:
        table_name = table[0]
        # DuckDB: pragma_table_info returns one row per column
        column_count = con.execute(f"""
            SELECT COUNT(*)
            FROM pragma_table_info('{table_name}');
        """).fetchone()[0]
        print(f"{table_name:<20} | {column_count:<15}")

def analyze_schema(con, tables):
    print_header("5 & 6. Schema & Columns")

    for table in tables:
        table_name = table[0]
        # Count columns
        col_count = con.execute(f"SELECT COUNT(*) FROM pragma_table_info('{table_name}')").fetchone()[0]

        print_subsection(f"Table: {table_name} ({col_count} columns)")
        print(f"{'Column Name':<25} | {'Data Type'}")

        cols = con.execute(f"SELECT name, type FROM pragma_table_info('{table_name}')").fetchall()
        for name, dtype in cols:
            print(f"{name:<25} | {dtype}")



def analyze_averages(con):
    """ Query 7: Calculate Averages (Only for logical numeric columns) """
    print_header("7. Business Averages")

    # 1. Average Sales (Train table)
    try:
        avg_sales = con.execute("SELECT AVG(unit_sales) FROM train").fetchone()[0]
        print(f"Average Unit Sales (train): {avg_sales:.4f}")
    except Exception as e:
        print(f"Could not calc average for train: {e}")

    # 2. Average Daily Transactions (Transactions table)
    try:
        avg_trans = con.execute("SELECT AVG(transactions) FROM transactions").fetchone()[0]
        print(f"Average Transactions (transactions): {avg_trans:.4f}")
    except Exception as e:
        print(f"Could not calc average for transactions: {e}")

    # 3. Average Oil Price (Oil table)
    try:
        avg_oil = con.execute("SELECT AVG(dcoilwtico) FROM oil").fetchone()[0]
        print(f"Average Oil Price (oil): {avg_oil:.4f}")
    except Exception as e:
        print(f"Could not calc average for oil: {e}")


def analyze_eda(con):
    """
    Query 8: Advanced Exploratory Data Analysis (EDA).
    Includes: Date Ranges, Cardinality (Distinct Counts), and Min/Max values.
    """
    print_header("8. Advanced Stats (EDA)")

    # Date Ranges (Time Series Scope)
    print_subsection("Date Ranges")
    for table in ['train', 'oil', 'holidays_events', 'transactions']:
        try:
            res = con.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
            print(f"Table {table:<15} | Start: {res[0]} | End: {res[1]}")
        except:
            pass

    # 2. Distinct Counts (Cardinality)
    print_subsection("Cardinality (Distinct Counts)")
    try:
        # How many cities and states are covered?
        n_cities = con.execute("SELECT COUNT(DISTINCT city) FROM stores").fetchone()[0]
        n_states = con.execute("SELECT COUNT(DISTINCT state) FROM stores").fetchone()[0]
        print(f"Geographic Coverage: {n_cities} cities across {n_states} states.")

        # How many product families?
        n_families = con.execute("SELECT COUNT(DISTINCT family) FROM items").fetchone()[0]
        print(f"Total product families (categories): {n_families}")
    except Exception as e:
        print(f"Error calculating distincts: {e}")

    # Extreme Values (Min/Max)
    print_subsection("Extreme Values")
    try:
        # Peak sales
        max_sales = con.execute("SELECT MAX(unit_sales) FROM train").fetchone()[0]
        print(f"Highest single recorded sale (train): {max_sales:,.2f}")

        # Oil price fluctuation
        min_oil, max_oil = con.execute("SELECT MIN(dcoilwtico), MAX(dcoilwtico) FROM oil").fetchone()
        print(f"Oil Price Range: {min_oil} - {max_oil}")
    except Exception as e:
        print(f"Error calculating extremes: {e}")


def analyze_yearly_distribution(con):
    """
    Query 9: Analyze Data Distribution by Year (Partitioning Analysis).
    Checks how records are distributed across time in the main table.
    """
    print_header("9. Data Distribution over Time")

    print_subsection("Row Counts by Year (Table: train)")

    try:
        # SQL Query to group by Year and count rows
        query = """
            SELECT 
                YEAR(date) as year,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) as pct
            FROM train
            GROUP BY year
            ORDER BY year
        """

        results = con.execute(query).fetchall()

        print(f"{'Year':<10} | {'Row Count':<15} | {'% of Total'}")
        print("-" * 45)

        for year, count, pct in results:
            print(f"{year:<10} | {count:<15,} | {pct}%")

    except Exception as e:
        print(f"Could not analyze distribution: {e}")


def analyze_ducklake_metadata_files(con, tables, schema: str = "main", max_print_per_table: int = 20):
    """
    Stage C: Query DuckLake metadata to list files for each table using ducklake_list_files,
    and verify that returned paths are RELATIVE (not absolute).

    Uses: FROM ducklake_list_files('catalog', 'table_name', schema => 'main');
    Docs: https://ducklake.select/docs/stable/duckdb/metadata/list_files.html
    """
    print_header("DuckLake METADATA - List Files (Relative Path Check)")

    catalog_name = "my_ducklake"  # matches ATTACH ... AS my_ducklake

    def is_absolute_path(p: str) -> bool:
        if p is None:
            return False
        p = str(p)
        # Windows absolute like "C:\..." or "C:/..."
        if len(p) >= 2 and p[1] == ":":
            return True
        # Unix absolute like "/..."
        if p.startswith("/"):
            return True
        return False

    for table in tables:
        table_name = table[0]
        print_subsection(f"Table: {table_name}")

        # Try with schema named param, fallback without (in case schema isn't supported in older versions)
        try:
            q = f"""
                SELECT
                    data_file,
                    delete_file
                FROM ducklake_list_files('{catalog_name}', '{table_name}', schema => '{schema}')
            """
            rows = con.execute(q).fetchall()
        except Exception:
            try:
                q = f"""
                    SELECT
                        data_file,
                        delete_file
                    FROM ducklake_list_files('{catalog_name}', '{table_name}')
                """
                rows = con.execute(q).fetchall()
            except Exception as e:
                print(f"Error querying metadata files: {e}")
                continue

        if not rows:
            print("No files returned by ducklake_list_files (could be empty table or no data files).")
            continue

        # Collect + check relativity
        all_paths = []
        abs_count = 0
        for data_file, delete_file in rows:
            if data_file:
                all_paths.append(str(data_file))
            if delete_file:
                all_paths.append(str(delete_file))

        for p in all_paths:
            if is_absolute_path(p):
                abs_count += 1

        rel_count = len(all_paths) - abs_count

        print(f"Total file paths returned: {len(all_paths)} | Relative: {rel_count} | Absolute: {abs_count}")

        # Print a sample (avoid flooding output)
        print(f"{'Path':<80} | {'Type':<10} | {'Relative?'}")
        print("-" * 110)

        printed = 0
        for data_file, delete_file in rows:
            if data_file and printed < max_print_per_table:
                p = str(data_file)
                print(f"{p:<80} | {'data':<10} | {'YES' if not is_absolute_path(p) else 'NO'}")
                printed += 1
            if delete_file and printed < max_print_per_table:
                p = str(delete_file)
                print(f"{p:<80} | {'delete':<10} | {'YES' if not is_absolute_path(p) else 'NO'}")
                printed += 1
            if printed >= max_print_per_table:
                break

        if abs_count > 0:
            print("\nWARNING: Some paths look ABSOLUTE. Requirement expects RELATIVE paths in metadata.")

def show_sample_rows(con, tables):
    """
    Shows 10 sample rows from each table in the lake.
    """
    print_header("10. Sample Data (First 10 Rows)")

    for table in tables:
        table_name = table[0]
        # Skip internal DuckDB/DuckLake system tables to keep output clean
        if table_name.startswith("ducklake_"): continue

        print_subsection(f"Table: {table_name}")
        try:
            # Fetch column names for better readability
            columns_info = con.execute(f"DESCRIBE {table_name}").fetchall()
            col_names = [col[0] for col in columns_info]
            # Print headers separated by pipes
            print(" | ".join(col_names))
            print("-" * (len(col_names) * 10))

            # Fetch the first 10 rows
            rows = con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
            for row in rows:
                # Convert values to string for safe printing
                print([str(val) for val in row])

        except Exception as e:
            print(f"Could not fetch sample for {table_name}: {e}")


# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == "__main__":
    con = None
    try:
        # Connect
        con = get_lake_connection()
        # Fetch tables list
        tables = con.execute("SHOW TABLES").fetchall()
        # Run Analysis
        analyze_table_count(con)
        analyze_row_counts(con, tables)
        analyze_nulls(con, tables)
        analyze_files(con)
        analyze_schema(con, tables)
        analyze_averages(con)
        analyze_eda(con)
        analyze_yearly_distribution(con)

        show_sample_rows(con, tables)
        analyze_ducklake_metadata_files(con, tables, schema="main", max_print_per_table=10)

        print_header("Analysis Complete")

    except Exception as e:
        print(f"\nError: {e}")
    finally:
        if con:
            con.close()