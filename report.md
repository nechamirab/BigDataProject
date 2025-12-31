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

## 10. Conclusion

This Data Lake provides a structured, scalable, and rich representation of retail sales data.
By utilizing **DuckDB** and **Parquet**, we successfully analyzed over **125 Million rows** efficiently. The analysis confirms
the data is clean, consistent (mostly 0% NULLs), and ready for advanced machine learning tasks.