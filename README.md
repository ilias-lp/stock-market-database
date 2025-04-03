# Analytical stock market database for major European companies

The project's scope is to create a locally hosted PostgreSQL database that comprises historical stock market data followed by daily update orchestrated by Apache Airflow and dashboarding of key market metrics in Metabase.
![project-outline](https://github.com/user-attachments/assets/d757a578-5749-4d09-9227-443f946686b6)

## Database Setup

Publicly traded companies considered to be included are European companies with market capitalization greater than 500 million euros as of end of March 2025. This results into a list of 1624 companies obtained from https://www.tradingview.com/ that also includes other columns with company metadata such as **company_name**, **sector**, **country**, **industry**, International Securities Identification Number (**isin**) and **symbol**. The last column with symbol is specific to Yahoo Finance symbol that was added separately (by making API calls with ISIN number and collecting corresponding symbol values, extracting financial stock data with ISIN number is not possible) since stock data will be extracted from Yahoo Finance by using yfinance Python library. List with companies and all metadata columns is stored in **./companies.csv**

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

Since /companies.csv has no currency column, it can be populated in Postgres with following command
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





























