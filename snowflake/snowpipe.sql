USE DATABASE ETL_PROJECT_DB;
USE SCHEMA FINANCIAL;
USE WAREHOUSE ETL_PROJECT_WH;

-- Snowpipe for AWS -> RAW_ACCOUNTS
CREATE OR REPLACE PIPE PIPE_ACCOUNTS
  AUTO_INGEST = FALSE
  AS
  COPY INTO RAW_ACCOUNTS (
    ACC_ID,
    ACC_NAME,
    TOTAL_SHARES,
    AUM,
    CASH_BALANCE,
    OPEN_DATE,
    CLOSE_DATE,
    AGE,
    GENDER,
    MOBILE_NUMBER,
    EMAIL,
    ACC_ADDRESS
  )
  FROM (
    SELECT
      $1,  -- acc_id
      $2,  -- acc_name
      $3,  -- total_shares
      $4,  -- AUM
      $5,  -- cash_balance
      $6,  -- open_date
      $7,  -- close_date
      $8,  -- age
      $9,  -- gender
      $10, -- mobile_number
      $11, -- email
      $12  -- acc_address
    FROM @AWS_STAGE
  )
  FILE_FORMAT = CSV_FORMAT;

-- Snowpipe for Azure -> RAW_TRANSACTIONS
CREATE OR REPLACE PIPE PIPE_TRANSACTIONS
  AUTO_INGEST = FALSE
  AS
  COPY INTO RAW_TRANSACTIONS (
    ACC_ID,
    SHARE_ID,
    TXN_TYPE,
    TXN_CODE,
    SHARE_PRICE,
    QUANTITY,
    TOTAL_AMOUNT,
    TXN_DATE,
    EXCHANGE
  )
  FROM (
    SELECT
      $1,  -- acc_id
      $2,  -- share_id
      $3,  -- txn_type
      $4,  -- txn_code
      $5,  -- share_price
      $6,  -- quantity
      $7,  -- total_amount
      $8,  -- txn_date
      $9   -- exchange
    FROM @AZURE_STAGE
  )
  FILE_FORMAT = CSV_FORMAT;

-- Snowpipe for GCP -> RAW_SHARES
CREATE OR REPLACE PIPE PIPE_SHARES
  AUTO_INGEST = FALSE
  AS
  COPY INTO RAW_SHARES (
    SHARE_ID,
    SHARE_NAME,
    SECTOR,
    TODAY_PRICE,
    YESTERDAY_PRICE,
    PRICE_CHANGE,
    MARKET_CAP,
    HIGH_52W,
    LOW_52W,
    LAST_UPDATED
  )
  FROM (
    SELECT
      $1,  -- share_id
      $2,  -- share_name
      $3,  -- sector
      $4,  -- today_price
      $5,  -- yesterday_price
      $6,  -- price_change
      $7,  -- market_cap
      $8,  -- 52w_high
      $9,  -- 52w_low
      $10  -- last_updated
    FROM @GCP_STAGE
  )
  FILE_FORMAT = CSV_FORMAT;

  -- Load accounts from AWS stage
ALTER PIPE PIPE_ACCOUNTS REFRESH;

-- Load transactions from Azure stage
ALTER PIPE PIPE_TRANSACTIONS REFRESH;

-- Load shares from GCP stage
ALTER PIPE PIPE_SHARES REFRESH;

-- Check RAW_ACCOUNTS
SELECT COUNT(*) FROM RAW_ACCOUNTS;

-- Check RAW_TRANSACTIONS
SELECT COUNT(*) FROM RAW_TRANSACTIONS;

-- Check RAW_SHARES
SELECT COUNT(*) FROM RAW_SHARES;



-- Note: Snowpipe setup for reference
-- In Phase 5, loading is triggered via
-- Stored Procedures (see 07_sp_load_procedures.sql)