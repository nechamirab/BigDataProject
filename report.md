# Data Lake Analysis Report - Stage C

## 1. Data Source

The data used in this project originates from the "Store Sales - Time Series Forecasting" competition.
The datasets were obtained from the official Kaggle open data portal:

🔗 **Link to data source:** [https://www.kaggle.com/c/store-sales-time-series-forecasting/data](https://www.kaggle.com/c/store-sales-time-series-forecasting/data)

The data was downloaded in its raw form (CSV) and processed into a structured Data Lake using DuckLake architecture.

---

## 2. Dataset Description

This Data Lake contains structured sales records and metadata published by **Corporación Favorita**, a large Ecuadorian grocery retailer. 

**What is in the dataset?**
The core of the dataset is the sales history of thousands of products across 54 stores in Ecuador. However, unlike simple sales records, this dataset is "Holistic" – it includes external factors that affect sales, such as:
* **Oil Prices:** Ecuador's economy is heavily dependent on oil; thus, oil prices affect consumer spending.
* **Holidays:** National and local holidays (which affect store traffic).
* **Transactions:** The volume of people visiting stores.

**Purpose:**
The primary purpose of this data is to train Machine Learning models for **Time Series Forecasting**, allowing the retailer to predict inventory needs and future sales.

---

## 3. Data Lake Structure

The Data Lake is organized as a DuckLake repository and stored in Parquet format.
The data is divided into multiple tables, each representing a logical entity within the dataset.

- Number of tables: **6**
- Storage format: **Parquet** (Columnar storage for efficient analytics)
- Data organization: **Lake (file-based storage partitioned by date)**

---

## 4. Physical Size and Volume

The following statistics describe the physical storage of the Data Lake:

- Total number of files in the lake: **239**
- Total size of all Parquet files: **860.47 MB**
- File types used:  
  - `.parquet` (Data files)

The size calculation was performed using SQL queries (`parquet_metadata`) over the physical files.

---

## 5. Basic Query Results (Full Data)

All queries below were executed on the **full dataset** using DuckDB SQL.

### 5.1 Number of Tables
- Total number of user tables in the Data Lake: **6**

### 5.2 Number of Rows per Table

| Table Name | Number of Rows | Description |
|-----------|----------------|-------------|
| **train** | **125,497,040** | The main table (huge volume of sales data). |
| **transactions** | **83,488** | Daily transaction counts per store. |
| **items** | **4,100** | Metadata about products. |
| **oil** | **1,218** | Daily oil prices. |
| **holidays_events** | **350** | Calendar of events. |
| **stores** | **54** | Metadata about the physical stores. |

**Total Number of Rows in Lake:** 125,586,250

---

### 5.3 NULL Values Analysis

We performed an SQL analysis to check for data quality issues (Missing Values).

* **oil table:** The column `dcoilwtico` contains **43** NULL values (**3.53%**). This likely represents weekends or days where oil was not traded.
* **train table:** The column `onpromotion` contains **21,657,651** NULL values (**17.26%**). This suggests that for older records, promotion data was not tracked.
* **Other tables:** `items`, `stores`, `transactions`, and `holidays_events` are clean (0% NULLs).

---

## 6. Detailed Schema and Column Descriptions

Below is a detailed explanation of the columns, their SQL data types, and what they represent.

### Table: `train` (Main Data)
| Column Name | Data Type | Meaning |
|------------|----------|-------------|
| `id` | **BIGINT** | A unique integer identifier for every record. |
| `date` | **DATE** | The specific day of the sale (YYYY-MM-DD). |
| `store_nbr` | **BIGINT** | Integer code identifying the store (Foreign Key to `stores`). |
| `item_nbr` | **BIGINT** | Integer code identifying the product (Foreign Key to `items`). |
| `unit_sales` | **DOUBLE** | The target variable. A numeric value (decimal) representing items sold. Can be fractional for items sold by weight. |
| `onpromotion` | **VARCHAR** | Indicates if the item was on sale. Stored as text/boolean. |

### Table: `stores` (Metadata)
| Column Name | Data Type | Meaning |
|------------|----------|-------------|
| `store_nbr` | **BIGINT** | Unique ID for the store. |
| `city` | **VARCHAR** | Text string representing the city name (e.g., "Quito"). |
| `state` | **VARCHAR** | Text string representing the state/province. |
| `type` | **VARCHAR** | A classification character (A, B, C, D, E) grouping similar stores. |
| `cluster` | **BIGINT** | An integer grouping of stores based on similarity. |

### Table: `oil` (Economic Data)
| Column Name | Data Type | Meaning |
|------------|----------|-------------|
| `date` | **DATE** | The date of the price reading. |
| `dcoilwtico` | **DOUBLE** | Decimal number representing the WTI Crude Oil price (e.g., 67.71). |

### Table: `items` (Product Metadata)
| Column Name | Data Type | Meaning |
|------------|----------|-------------|
| `item_nbr` | **BIGINT** | Unique product ID. |
| `family` | **VARCHAR** | Text category of the product (e.g., "GROCERY I", "AUTOMOTIVE"). |
| `class` | **BIGINT** | Integer code for finer product classification. |
| `perishable` | **BIGINT** | Boolean flag (0 or 1) indicating if the item spoils quickly. |

---

## 7. Business Metrics (Averages)

To understand the scale of operations, we calculated averages using SQL `AVG()`:

| Metric | Table | Average Value | Meaning                                                           |
| :--- | :--- | :--- |:------------------------------------------------------------------|
| **Daily Oil Price** | `oil` | **67.71** | The average oil price was ~67$ during the dataset period.          |
| **Unit Sales** | `train` | **8.55** | On average, ~8.5 units of a specific item are sold per store/day. |
| **Daily Transactions** | `transactions` | **1,694.60** | An average store processes ~1,694 customer transactions daily.    |

---

## 8. Advanced Basic Stats (Exploratory Data Analysis)

Beyond basic counts, we performed advanced SQL analysis to understand the scope and ranges of the data.

### 8.1 Time Range Analysis (Temporal Scope)
We queried the `MIN` and `MAX` dates to understand the coverage:
* **Training Data:** Covers **2013-01-01** to **2017-08-15** (approx. 4.5 years).
* **Oil Data:** Extends slightly further to **2017-08-31**.
* **Holidays:** Covers a wider range from **2012** to **2017**.

### 8.2 Cardinality & Diversity (Geographic & Product Scope)
Using `COUNT(DISTINCT ...)` queries, we mapped the business size:
* **Geographic Coverage:** The chain operates in **22 unique cities** across **16 states**.
* **Product Variety:** The dataset categorizes products into **33 unique families** (Categories).

### 8.3 Extreme Values
* **Highest Sale:** The maximum number of units sold for a single item in one day was **89,440.00**.
* **Oil Volatility:** Oil prices fluctuated significantly between a low of **26.19\$** and a high of **110.62\$**, highlighting the economic variance during this period.

---

## 9. Data Distribution over Time

We analyzed how the records in the main `train` table are distributed across the years. This validates that the partitioning logic is working correctly and shows the growth of data volume.

| Year | Row Count | % of Total |
|------|-----------|------------|
| 2013 | 16,322,662 | 13.01% |
| 2014 | 22,271,602 | 17.75% |
| 2015 | 27,864,644 | 22.20% |
| 2016 | 35,229,871 | 28.07% |
| 2017 | 23,808,261 | 18.97% |

*Note: The drop in 2017 is due to the dataset ending in August 2017, meaning it does not cover a full year like the previous ones.*

---

## 10. Sample Data Snapshot

To validate data integrity, schema consistency, and correct type parsing during the ingestion process, we queried a sample of **10 rows** from each table in the Data Lake.

### Table: `holidays_events`
| type | locale | locale_name | description | transferred | date |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Holiday | Local | Manta | Fundacion de Manta | False | 2012-03-02 |
| Holiday | Regional | Cotopaxi | Provincializacion de Cotopaxi | False | 2012-04-01 |
| Holiday | Local | Cuenca | Fundacion de Cuenca | False | 2012-04-12 |
| Holiday | Local | Libertad | Cantonizacion de Libertad | False | 2012-04-14 |
| Holiday | Local | Riobamba | Cantonizacion de Riobamba | False | 2012-04-21 |
| Holiday | Local | Puyo | Cantonizacion del Puyo | False | 2012-05-12 |
| Holiday | Local | Guaranda | Cantonizacion de Guaranda | False | 2012-06-23 |
| Holiday | Regional | Imbabura | Provincializacion de Imbabura | False | 2012-06-25 |
| Holiday | Local | Latacunga | Cantonizacion de Latacunga | False | 2012-06-25 |
| Holiday | Local | Machala | Fundacion de Machala | False | 2012-06-25 |

### Table: `items`
| item_nbr | family | class | perishable |
| :--- | :--- | :--- | :--- |
| 96995 | GROCERY I | 1093 | 0 |
| 99197 | GROCERY I | 1067 | 0 |
| 103501 | CLEANING | 3008 | 0 |
| 103520 | GROCERY I | 1028 | 0 |
| 103665 | BREAD/BAKERY | 2712 | 1 |
| 105574 | GROCERY I | 1045 | 0 |
| 105575 | GROCERY I | 1045 | 0 |
| 105576 | GROCERY I | 1045 | 0 |
| 105577 | GROCERY I | 1045 | 0 |
| 105693 | GROCERY I | 1034 | 0 |

### Table: `oil`
| dcoilwtico | date |
| :--- | :--- |
| None | 2013-01-01 |
| 93.14 | 2013-01-02 |
| 92.97 | 2013-01-03 |
| 93.12 | 2013-01-04 |
| 93.2 | 2013-01-07 |
| 93.21 | 2013-01-08 |
| 93.08 | 2013-01-09 |
| 93.81 | 2013-01-10 |
| 93.6 | 2013-01-11 |
| 94.27 | 2013-01-14 |

### Table: `stores`
| store_nbr | city | state | type | cluster |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Quito | Pichincha | D | 13 |
| 2 | Quito | Pichincha | D | 13 |
| 3 | Quito | Pichincha | D | 8 |
| 4 | Quito | Pichincha | D | 9 |
| 5 | Santo Domingo | Santo Domingo de los Tsachilas | D | 4 |
| 6 | Quito | Pichincha | D | 13 |
| 7 | Quito | Pichincha | D | 8 |
| 8 | Quito | Pichincha | D | 8 |
| 9 | Quito | Pichincha | B | 6 |
| 10 | Quito | Pichincha | C | 15 |

### Table: `transactions`
| store_nbr | transactions | date |
| :--- | :--- | :--- |
| 25 | 770 | 2013-01-01 |
| 1 | 2111 | 2013-01-02 |
| 2 | 2358 | 2013-01-02 |
| 3 | 3487 | 2013-01-02 |
| 4 | 1922 | 2013-01-02 |
| 5 | 1903 | 2013-01-02 |
| 6 | 2143 | 2013-01-02 |
| 7 | 1874 | 2013-01-02 |
| 8 | 3250 | 2013-01-02 |
| 9 | 2940 | 2013-01-02 |

### Table: `train` (Main Fact Table)
| id | date | store_nbr | item_nbr | unit_sales | onpromotion |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 0 | 2013-01-01 | 25 | 103665 | 7.0 | None |
| 1 | 2013-01-01 | 25 | 105574 | 1.0 | None |
| 2 | 2013-01-01 | 25 | 105575 | 2.0 | None |
| 3 | 2013-01-01 | 25 | 108079 | 1.0 | None |
| 4 | 2013-01-01 | 25 | 108701 | 1.0 | None |
| 5 | 2013-01-01 | 25 | 108786 | 3.0 | None |
| 6 | 2013-01-01 | 25 | 108797 | 1.0 | None |
| 7 | 2013-01-01 | 25 | 108952 | 1.0 | None |
| 8 | 2013-01-01 | 25 | 111397 | 13.0 | None |
| 9 | 2013-01-01 | 25 | 114790 | 3.0 | None |

---

## 11. Metadata & Relative Path Verification

A critical requirement for Data Lake portability is ensuring that file paths stored in the metadata are **Relative** rather than Absolute. We queried the internal DuckLake function `ducklake_list_files` to verify this.

**Verification Results:**
The analysis confirms that **100%** of the paths in the metadata are correctly stored as relative paths.

| Table Analyzed | Total Files | Relative Paths | Absolute Paths | Status |
| :--- | :--- | :--- | :--- | :--- |
| **holidays_events** | 69 | 69 | 0 | ✅ PASS |
| **items** | 1 | 1 | 0 | ✅ PASS |
| **oil** | 56 | 56 | 0 | ✅ PASS |
| **stores** | 1 | 1 | 0 | ✅ PASS |
| **train** | 56 | 56 | 0 | ✅ PASS |
| **transactions** | 56 | 56 | 0 | ✅ PASS |

**Sample Metadata Output (Proof of Relativity):**
The following is a snippet from the metadata query showing the file structure.

| Path (Snippet) | Type | Relative? |
| :--- | :--- | :--- |
| `full_ducklake\my_ducklake.ducklake.files\\main\holidays_events\year=2012\month=3\...parquet` | data | **YES** |
| `full_ducklake\my_ducklake.ducklake.files\\main\items\ducklake-019b8ecc-3d02...parquet` | data | **YES** |
| `full_ducklake\my_ducklake.ducklake.files\\main\train\year=2013\month=1\ducklake...parquet` | data | **YES** |

---

## 12. Conclusion

This Data Lake provides a structured, scalable, and rich representation of retail sales data.
By utilizing **DuckDB** and **Parquet**, we successfully analyzed over **125 Million rows** efficiently. The analysis confirms
the data is clean, consistent (mostly 0% NULLs), and ready for advanced machine learning tasks.