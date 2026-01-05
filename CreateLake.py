import duckdb
from pathlib import Path
import shutil

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Set to True for Submission (Sample < 50MB), False for Full Local Work (~5GB)
CREATE_SAMPLE_FOR_SUBMISSION = False

# Paths configuration
BASE_DIR = Path(".")
DATA_SOURCE_DIR = BASE_DIR / "favorita-grocery-sales-forecasting"

# Define output directory based on the mode (Sample vs Full)
if CREATE_SAMPLE_FOR_SUBMISSION:
    OUTPUT_DIR = BASE_DIR / "submission_ducklake"
    SAMPLE_RATE = "1%"
    MODE_DESC = "CREATING SAMPLE (<50MB)"
else:
    OUTPUT_DIR = BASE_DIR / "full_ducklake"
    SAMPLE_RATE = "100%"
    MODE_DESC = "CREATING FULL LAKE"

# Define specific DuckLake filenames required for submission
LAKE_METADATA_FILE = OUTPUT_DIR / "my_ducklake.ducklake"
LAKE_FILES_DIR = OUTPUT_DIR / "my_ducklake.ducklake.files"

FILES = {
    "train": DATA_SOURCE_DIR / "train.csv",
    "items": DATA_SOURCE_DIR / "items.csv",
    "stores": DATA_SOURCE_DIR / "stores.csv",
    "oil": DATA_SOURCE_DIR / "oil.csv",
    "holidays": DATA_SOURCE_DIR / "holidays_events.csv",
    "transactions": DATA_SOURCE_DIR / "transactions.csv"
}

# ==============================================================================
# UI HELPERS
# ==============================================================================
def print_header(title):
    print(f"\n{'=' * 60}")
    print(f"--- {title} ---")
    print(f"{'=' * 60}")

def print_step(title):
    print(f"\n>> {title}...")

# ==============================================================================
# LOGIC
# ==============================================================================
def get_date_expression(col_name):
    """
    Returns a SQL expression to safely parse dates.
    Handles both 'YYYY-MM-DD' and 'DD/MM/YYYY' formats found in CSVs.
    """
    return f"""
    COALESCE(
        CAST(TRY_STRPTIME(CAST({col_name} AS VARCHAR), '%Y-%m-%d') AS DATE),
        CAST(TRY_STRPTIME(CAST({col_name} AS VARCHAR), '%d/%m/%Y') AS DATE)
    )
    """


def init_lake() -> duckdb.DuckDBPyConnection:
    """
    Initializes the environment and returns the active DuckDB connection object.
    """
    print_header(f"INITIALIZATION: {MODE_DESC}")
    # Clean up old runs
    if OUTPUT_DIR.exists():
        print(f"Cleaning old directory: {OUTPUT_DIR}")
        try:
            shutil.rmtree(OUTPUT_DIR)
        except Exception as e:
            print(f"Warning: Could not delete old folder: {e}")

    # Create necessary folders
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LAKE_FILES_DIR.mkdir(parents=True, exist_ok=True)

    # Create Connection
    con = duckdb.connect()
    # Install & Load Extension
    con.execute("INSTALL ducklake; LOAD ducklake;")
    # Attach Lake
    # We use DATA_PATH to separate metadata from the heavy parquet files
    print(f"Attaching lake to: {LAKE_METADATA_FILE}")
    con.execute(f"""
        ATTACH 'ducklake:{LAKE_METADATA_FILE.as_posix()}' AS my_ducklake
        (DATA_PATH '{LAKE_FILES_DIR.as_posix()}');
    """)
    # Set context to the lake
    con.execute("USE my_ducklake;")
    return con

def load_data(con: duckdb.DuckDBPyConnection):
    """
    Loads all CSV files into the DuckLake.
    Handles Partitioning (Year/Month) and Sampling if needed.
    """
    print_header("DATA LOADING PHASE")

    # Check if files exist
    for name, path in FILES.items():
        if not path.exists():
            print(f"ERROR: File not found: {path}")
            return

    # Load Dimension Tables (Small files - No Sampling)
    print_step("Loading Dimensions (items, stores)")
    con.execute(f"CREATE OR REPLACE TABLE items AS SELECT * FROM read_csv_auto('{FILES['items'].as_posix()}');")
    con.execute(f"CREATE OR REPLACE TABLE stores AS SELECT * FROM read_csv_auto('{FILES['stores'].as_posix()}');")

    # Load Time-Series Tables
    for table, fpath in [('oil', 'oil'), ('holidays_events', 'holidays'), ('transactions', 'transactions')]:
        print_step(f"Loading & Partitioning '{table}'")
        con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS 
                SELECT * EXCLUDE(date), {get_date_expression("date")} AS date
                FROM read_csv_auto('{FILES[fpath].as_posix()}') WHERE 1=0;
            """)
        con.execute(f"ALTER TABLE {table} SET PARTITIONED BY (year(date), month(date));")
        con.execute(f"""
                INSERT INTO {table}
                SELECT * EXCLUDE(date), {get_date_expression("date")} AS date
                FROM read_csv_auto('{FILES[fpath].as_posix()}');
            """)

    # Load Main Fact Table (TRAIN) - Needs Sampling & Partitioning
    print_step(f"Loading 'train' (Rate: {SAMPLE_RATE})")
    # 'bernoulli' sampling scans the whole file ensuring good distribution
    sample_sql = f"USING SAMPLE {SAMPLE_RATE} (bernoulli)" if CREATE_SAMPLE_FOR_SUBMISSION else ""

    con.execute(f"""
                CREATE OR REPLACE TABLE train AS
                SELECT id, {get_date_expression("date")} AS date, store_nbr, item_nbr, unit_sales, onpromotion
                FROM read_csv_auto('{FILES['train'].as_posix()}') WHERE 1=0; 
            """)
    con.execute("ALTER TABLE train SET PARTITIONED BY (year(date), month(date));")

    print("...Inserting data (this may take time)...")
    con.execute(f"""
                INSERT INTO train
                SELECT id, {get_date_expression("date")} AS date, store_nbr, item_nbr, unit_sales, onpromotion
                FROM read_csv_auto('{FILES['train'].as_posix()}') {sample_sql}; 
            """)

    '''print_step("Generating Metadata Table (files list)")
    glob_pattern = f"{LAKE_FILES_DIR.as_posix()}/**/*.parquet"
    con.execute(f"""
            CREATE OR REPLACE TABLE my_ducklake.files AS 
            SELECT * FROM glob('{glob_pattern}')
        """)
    print(">> Metadata table 'files' created successfully.")'''

def verify_lake(con):
    print_header("VERIFICATION")
    print(con.execute("SHOW TABLES;").fetchdf())

    count = con.execute("SELECT COUNT(*) FROM train").fetchone()[0]
    print(f"\nTotal rows in 'train': {count:,}")

    dates = con.execute("SELECT MIN(date), MAX(date) FROM train").fetchone()
    print(f"Date Range: {dates[0]} to {dates[1]}")

    if CREATE_SAMPLE_FOR_SUBMISSION:
        if count > 2_000_000:
            print("WARNING: Sample size might be too large (>2M rows). Check output size!")
        else:
            print("Sample size looks good for submission.")


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    con = init_lake()

    try:
        load_data(con)
        verify_lake(con)

    except Exception as e:
        print(f"\nERROR: {e}")
    finally:
        con.close()