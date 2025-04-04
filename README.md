# Analytical stock market database for major European companies

The project's scope is to create a locally hosted PostgreSQL database that comprises historical stock market data followed by daily updates orchestrated by Apache Airflow and dashboarding of custom market metrics in Metabase. 

Code was written and tested on Ubuntu 24.04.2 LTS. Packages from **./requirements.txt** were installed and run in Python 3.10 virtual environment whereas latest versions of PostgreSQL and Metabase were installed and used system globally.

![project-outline](https://github.com/user-attachments/assets/d757a578-5749-4d09-9227-443f946686b6)

## Database Setup

Publicly traded companies considered to be included are European companies with market capitalization greater than 500 million euros as of end of March 2025. This results into a list of 1624 companies obtained from https://www.tradingview.com/ that also includes other columns with company metadata such as **company_name**, **sector**, **country**, **industry**, International Securities Identification Number (**isin**) and **symbol**. 

The last column with symbol is specific to Yahoo Finance symbol that was added separately (by making API calls with ISIN number and collecting corresponding symbol values; extracting financial stock data with ISIN number is not possible). Financial stock data will be extracted from Yahoo Finance (https://finance.yahoo.com/markets/) by using yfinance Python library. List with companies and all metadata columns is stored in **./companies.csv**

![companies](https://github.com/user-attachments/assets/2e6812ae-7205-48f8-b13f-21fc8318aa27)

PostgreSQL *stockmarket* database will have 2 tables, one with companies metadata and the other with daily financial data.

![database](https://github.com/user-attachments/assets/a2bad8d2-5c3d-4370-8303-56b3c4448ec0)


```sql
CREATE DATABASE stockmarket;

CREATE USER admin WITH PASSWORD 'password';

GRANT ALL PRIVILEGES ON DATABASE stockmarket TO admin;

CREATE TABLE companies (
    symbol VARCHAR(12) PRIMARY KEY,
    isin VARCHAR(12),
    company_name VARCHAR(100),
    country VARCHAR(50),
    sector VARCHAR(50),
    industry VARCHAR(50),
    market_cap NUMERIC(20, 0),
    currency VARCHAR(3)
);

CREATE TABLE stock_data (
    symbol VARCHAR(12) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4),
    volume BIGINT,
    PRIMARY KEY (symbol, date),
    CONSTRAINT fk_company 
        FOREIGN KEY (symbol) 
        REFERENCES companies(symbol)
        ON DELETE CASCADE
);

CREATE INDEX idx_stock_data_symbol ON stock_data(symbol);

CREATE INDEX idx_stock_data_date ON stock_data(date);
```

Companies table is made by running **./postgres_companies_table.py**

Since **./companies.csv** has no currency column, it can be populated in Postgres with following command
```sql
UPDATE companies
SET currency = CASE
    WHEN country = 'Cyprus' THEN 'EUR' -- Euro
    WHEN country = 'Switzerland' THEN 'CHF' -- Swiss Franc
    WHEN country = 'Italy' THEN 'EUR' -- Euro
    WHEN country = 'Hungary' THEN 'HUF' -- Hungarian Forint
    WHEN country = 'Luxembourg' THEN 'EUR' -- Euro
    WHEN country = 'Czech Republic' THEN 'CZK' -- Czech Koruna
    WHEN country = 'Norway' THEN 'NOK' -- Norwegian Krone
    WHEN country = 'Sweden' THEN 'SEK' -- Swedish Krona
    WHEN country = 'United Kingdom' THEN 'GBP' -- British Pound
    WHEN country = 'Netherlands' THEN 'EUR' -- Euro
    WHEN country = 'Romania' THEN 'RON' -- Romanian Leu
    WHEN country = 'Austria' THEN 'EUR' -- Euro
    WHEN country = 'Ireland' THEN 'EUR' -- Euro
    WHEN country = 'Germany' THEN 'EUR' -- Euro
    WHEN country = 'Portugal' THEN 'EUR' -- Euro
    WHEN country = 'Finland' THEN 'EUR' -- Euro
    WHEN country = 'Malta' THEN 'EUR' -- Euro
    WHEN country = 'Lithuania' THEN 'EUR' -- Euro
    WHEN country = 'Spain' THEN 'EUR' -- Euro
    WHEN country = 'Liechtenstein' THEN 'CHF' -- Swiss Franc
    WHEN country = 'Slovenia' THEN 'EUR' -- Euro
    WHEN country = 'Greece' THEN 'EUR' -- Euro
    WHEN country = 'Belgium' THEN 'EUR' -- Euro
    WHEN country = 'France' THEN 'EUR' -- Euro
    WHEN country = 'Estonia' THEN 'EUR' -- Euro
    WHEN country = 'Slovakia' THEN 'EUR' -- Euro
    WHEN country = 'Monaco' THEN 'EUR' -- Euro
    WHEN country = 'Iceland' THEN 'ISK' -- Icelandic Króna
    WHEN country = 'Poland' THEN 'PLN' -- Polish Złoty
    WHEN country = 'Denmark' THEN 'DKK' -- Danish Krone
    ELSE NULL -- Fallback for any unexpected values
END
WHERE currency IS NULL; -- Only update empty currency fields
```
Next, historical financial data from Yahoo Finance will be extracted for each stock for a desired period of time with **./api_extract_historical.py** where used start_date is 2019-01-01 which results in 5+ years. Each stock's data will be stored in a separate csv file in **./stock_data/** subfolder.

After extraction is completed, data from csv files can be imported to Postgres with **./postgres_import_stock_data.py** that concludes the database setup resulting in 2.4 million rows of financial data for 1624 companies.

![count](https://github.com/user-attachments/assets/1aa15c3d-4773-48c6-867a-2bc03da2b7a4)

## Daily updates

Automatic daily updates are scheduled and executed in Apache Airflow. **./airflow/dags/stock_data_updater.py** collects missing daily data up until today at 18.00 and updates the database.

**./airflow/airflow.cfg** that gets created after Airflow database initialization must have following parameters for connection and smooth running:

```
executor = LocalExecutor

sql_alchemy_conn = postgresql+psycopg2://admin:password@localhost:5432/stockmarket

max_active_tasks_per_dag = 1

max_active_runs_per_dag = 1

load_examples = False     # Optional for removing example DAGs from the workspace
```

## Dashboarding

Metabase (https://github.com/metabase/metabase) is an open sources business intelligence tool for data visualizing that is optimal for local PostgreSQL setup and easy to install and configure locally.

Numerous custom stock market metrics can be derived and visualized by using regular SQL queries. Several examples are given below.

**Top gainers/losers (last trading day)**
```sql
WITH latest_records AS (
    SELECT 
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close
    FROM stock_data
),
latest_date AS (
    SELECT MAX(date) as md FROM stock_data
)
SELECT 
    s.symbol,
    c.company_name,
    c.sector,
    c.market_cap,
    s.date,
    s.close as latest_price,
    ROUND(((s.close - s.prev_close) / s.prev_close * 100), 2) AS daily_change_pct
FROM latest_records s
JOIN companies c ON s.symbol = c.symbol
JOIN latest_date ld ON s.date = ld.md
WHERE s.prev_close IS NOT NULL
ORDER BY ABS(ROUND(((s.close - s.prev_close) / s.prev_close * 100), 2)) DESC
LIMIT 20;
```

![daily_top](https://github.com/user-attachments/assets/ac65fa91-9e51-4749-86e2-f54af5136727)

**Sector performance overview (last 30 days)**
```sql
WITH sector_data AS (
    SELECT 
        c.sector,
        s.symbol,
        FIRST_VALUE(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date) AS start_price,
        LAST_VALUE(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price
    FROM stock_data s
    JOIN companies c ON s.symbol = c.symbol
    WHERE s.date >= (SELECT MAX(date) - INTERVAL '30 days' FROM stock_data)
    )
SELECT
    sector,
    COUNT(DISTINCT symbol) AS num_companies,
    ROUND(AVG((end_price - start_price) / start_price * 100), 2) AS avg_return_pct,
    ROUND(STDDEV((end_price - start_price) / start_price * 100), 2) AS volatility_pct
FROM sector_data
GROUP BY sector
ORDER BY avg_return_pct DESC
```

![sector](https://github.com/user-attachments/assets/5f26bb08-941f-4b32-8b33-15265e2b41b7)

**Top volume surges (compared to 30-day average)**
```sql
WITH latest_stats AS (
    SELECT 
        s.symbol,
        s.volume,
        s.date,
        c.company_name,
        c.sector,
        AVG(s2.volume) OVER (PARTITION BY s.symbol) AS avg_30day_volume
    FROM stock_data s
    JOIN companies c ON s.symbol = c.symbol
    JOIN stock_data s2 ON s.symbol = s2.symbol 
        AND s2.date BETWEEN s.date - INTERVAL '30 days' AND s.date
    WHERE s.date = (SELECT MAX(date) FROM stock_data)
    )
SELECT 
    DISTINCT symbol,
    company_name,
    sector,
    date,
    volume AS latest_volume,
    ROUND(avg_30day_volume) AS avg_30day_volume,
    ROUND((volume / avg_30day_volume)::numeric, 2) AS volume_ratio
FROM latest_stats
WHERE avg_30day_volume > 10000
ORDER BY volume_ratio DESC
LIMIT 20
```

![volume_top](https://github.com/user-attachments/assets/b2ac66a4-7c13-41d9-9557-6bfdbcc77b6f)
























