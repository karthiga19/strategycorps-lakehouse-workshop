-- ============================================================================
-- Strategy Corps banking medallion — Lakeflow Declarative Pipeline (DLT)
-- ============================================================================
--
-- This is the REFERENCE / answer key for Leg 2. In the session you prompt Genie
-- Code to write a pipeline like this and then create the pipeline; keep this
-- file to compare against, or as a catch-up.
--
-- It ingests the raw files that setup/00_setup landed in your Volume and builds
-- the full medallion in one declarative graph:
--
--   raw files (Volume)
--     -> BRONZE  streaming tables, Auto Loader, raw/as-landed
--     -> SILVER  materialized views: typed, deduped, joined, EXPECTATIONS
--     -> GOLD    materialized views: business-ready facts + aggregates
--
-- Why a pipeline and not notebooks?
--   * Auto Loader (read_files with STREAM) ingests only new files incrementally.
--   * EXPECT constraints make data quality declarative and observable — the
--     pipeline UI shows how many rows each expectation dropped, every run.
--   * The dependency graph (bronze -> silver -> gold) is inferred and shown,
--     giving you lineage for free.
--
-- Pipeline configuration to set when you create the pipeline:
--   landing = /Volumes/<catalog>/<your_schema>/landing
--   (and set the pipeline's default catalog + target schema to your own)
-- ============================================================================


-- ---------------------------------------------------------------------------
-- BRONZE — raw, as-landed. Streaming tables read the Volume with Auto Loader.
-- No casting, no cleaning: bronze is a faithful copy of the source.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_customers
  COMMENT 'Raw customer records as landed (all strings, includes duplicates and planted defects).'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing}/customers/',
  format => 'csv', header => 'true', inferColumnTypes => 'false'
);

CREATE OR REFRESH STREAMING TABLE bronze_accounts
  COMMENT 'Raw account records as landed.'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing}/accounts/',
  format => 'csv', header => 'true', inferColumnTypes => 'false'
);

CREATE OR REFRESH STREAMING TABLE bronze_branches
  COMMENT 'Raw branch dimension as landed.'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing}/branches/',
  format => 'csv', header => 'true', inferColumnTypes => 'false'
);

CREATE OR REFRESH STREAMING TABLE bronze_customer_events
  COMMENT 'Raw customer lifecycle events as landed (acquired / attrited / reactivated).'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing}/customer_events/',
  format => 'csv', header => 'true', inferColumnTypes => 'false'
);

CREATE OR REFRESH STREAMING TABLE bronze_card_transactions
  COMMENT 'Raw card transactions as landed (JSON shards, includes duplicates and 100x typos).'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing}/card_transactions/',
  format => 'json'
);


-- ---------------------------------------------------------------------------
-- SILVER — typed, deduped, joined, with EXPECTATIONS. Rows that violate an
-- expectation are DROPPED from the clean table; a parallel quarantine view
-- keeps them with a reason so nothing is lost silently.
-- ---------------------------------------------------------------------------

-- Branches: typed, no dedup needed.
CREATE OR REFRESH MATERIALIZED VIEW silver_branches
  COMMENT 'Typed branch dimension.'
AS SELECT
  branch_id,
  branch_name,
  branch_state,
  branch_region,
  CAST(opened_date AS DATE) AS opened_date
FROM bronze_branches;

-- Customers: dedup on customer_id (keep latest updated_at), parse both date
-- formats, standardize state, derive age + age_band. Expectations drop rows we
-- cannot use.
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  CONSTRAINT valid_dob        EXPECT (date_of_birth IS NOT NULL)         ON VIOLATION DROP ROW,
  CONSTRAINT plausible_age    EXPECT (age BETWEEN 18 AND 110)            ON VIOLATION DROP ROW,
  CONSTRAINT known_state      EXPECT (home_state IS NOT NULL)
)
  COMMENT 'One row per customer: deduped, typed, state-standardized, age derived.'
AS
WITH deduped AS (
  SELECT * FROM bronze_customers
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY CAST(updated_at AS DATE) DESC
  ) = 1
),
parsed AS (
  SELECT
    customer_id,
    first_name,
    last_name,
    -- date_of_birth arrives as ISO (yyyy-MM-dd) OR MM/dd/yyyy — try both.
    COALESCE(TRY_TO_DATE(date_of_birth), TRY_TO_DATE(date_of_birth, 'MM/dd/yyyy')) AS date_of_birth,
    gender,
    -- Standardize state to a 2-letter code (data mixes 'TN' with 'Tennessee').
    CASE UPPER(home_state)
      WHEN 'TENNESSEE' THEN 'TN' WHEN 'TEXAS' THEN 'TX' WHEN 'GEORGIA' THEN 'GA'
      WHEN 'FLORIDA' THEN 'FL' WHEN 'NORTH CAROLINA' THEN 'NC' WHEN 'OHIO' THEN 'OH'
      WHEN 'ILLINOIS' THEN 'IL' WHEN 'MICHIGAN' THEN 'MI' WHEN 'CALIFORNIA' THEN 'CA'
      WHEN 'ARIZONA' THEN 'AZ' WHEN 'NEW YORK' THEN 'NY' WHEN 'PENNSYLVANIA' THEN 'PA'
      ELSE UPPER(home_state)
    END AS home_state,
    home_zip,
    home_branch_id,
    segment,
    acquisition_channel,
    NULLIF(source_bank, '') AS source_bank,
    CAST(join_date AS DATE) AS join_date,
    customer_status,
    CAST(attrition_flag AS INT) AS attrition_flag,
    CAST(updated_at AS DATE) AS updated_at
  FROM deduped
)
SELECT
  *,
  FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) AS age,
  CASE
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) BETWEEN 18 AND 25 THEN '18-25'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) BETWEEN 26 AND 35 THEN '26-35'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) BETWEEN 36 AND 45 THEN '36-45'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) BETWEEN 46 AND 55 THEN '46-55'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) BETWEEN 56 AND 65 THEN '56-65'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) BETWEEN 66 AND 75 THEN '66-75'
    ELSE '76+'
  END AS age_band,
  ROUND(DATEDIFF(current_date(), join_date) / 365.25, 2) AS tenure_years,
  (acquisition_channel = 'switch_kit') AS moved_from_bank,
  (customer_status <> 'attrited') AS is_retained
FROM parsed;

-- Quarantine: customers dropped by silver, with the reason. Same parse logic,
-- inverse predicate — this is the "keep the bad rows" half of an expectation.
CREATE OR REFRESH MATERIALIZED VIEW silver_customers_quarantine
  COMMENT 'Customer rows that failed a silver expectation, with the reason.'
AS
WITH deduped AS (
  SELECT * FROM bronze_customers
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY CAST(updated_at AS DATE) DESC
  ) = 1
),
parsed AS (
  SELECT
    customer_id,
    COALESCE(TRY_TO_DATE(date_of_birth), TRY_TO_DATE(date_of_birth, 'MM/dd/yyyy')) AS dob,
    date_of_birth AS dob_raw
  FROM deduped
)
SELECT customer_id, dob_raw,
  CASE
    WHEN dob IS NULL THEN 'missing_or_unparseable_dob'
    WHEN FLOOR(DATEDIFF(current_date(), dob) / 365.25) NOT BETWEEN 18 AND 110 THEN 'implausible_age'
  END AS quarantine_reason
FROM parsed
WHERE dob IS NULL
   OR FLOOR(DATEDIFF(current_date(), dob) / 365.25) NOT BETWEEN 18 AND 110;

-- Accounts: typed, quarantine negative balances, 100x outliers and orphans.
CREATE OR REFRESH MATERIALIZED VIEW silver_accounts (
  CONSTRAINT non_negative_balance EXPECT (balance >= 0)              ON VIOLATION DROP ROW,
  CONSTRAINT plausible_balance    EXPECT (balance < 10000000)        ON VIOLATION DROP ROW,
  CONSTRAINT has_customer         EXPECT (customer_id IS NOT NULL)   ON VIOLATION DROP ROW
)
  COMMENT 'One row per account: typed, valid balances, linked to a real customer.'
AS SELECT
  a.account_id,
  a.customer_id,
  a.account_type,
  CAST(a.open_date AS DATE) AS open_date,
  a.account_status,
  CAST(a.balance AS DOUBLE) AS balance
FROM bronze_accounts a
-- Orphan accounts (customer_id in no customer record at all) fail this join and
-- are excluded. Checked against the full customer master (bronze), not silver,
-- so an account is only an "orphan" if its customer truly does not exist — not
-- merely because the customer was dropped downstream for a bad DOB.
SEMI JOIN (SELECT DISTINCT customer_id FROM bronze_customers) c
  ON a.customer_id = c.customer_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_accounts_quarantine
  COMMENT 'Account rows that failed a silver expectation, with the reason.'
AS SELECT
  a.account_id, a.customer_id, CAST(a.balance AS DOUBLE) AS balance,
  CASE
    WHEN c.customer_id IS NULL THEN 'orphan_customer_id'
    WHEN CAST(a.balance AS DOUBLE) < 0 THEN 'negative_balance'
    WHEN CAST(a.balance AS DOUBLE) >= 10000000 THEN 'balance_outlier_100x'
  END AS quarantine_reason
FROM bronze_accounts a
LEFT JOIN (SELECT DISTINCT customer_id FROM bronze_customers) c
  ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL
   OR CAST(a.balance AS DOUBLE) < 0
   OR CAST(a.balance AS DOUBLE) >= 10000000;

-- Card transactions: dedup exact duplicate txn_id, type, drop 100x amount typos.
CREATE OR REFRESH MATERIALIZED VIEW silver_card_transactions (
  CONSTRAINT plausible_amount EXPECT (amount < 100000) ON VIOLATION DROP ROW
)
  COMMENT 'One row per transaction: deduped, typed, outliers dropped.'
AS SELECT
  txn_id,
  account_id,
  card_type,
  channel,
  merchant_category,
  CAST(amount AS DOUBLE) AS amount,
  CAST(txn_ts AS DATE) AS txn_date
FROM bronze_card_transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_ts) = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver_customer_events
  COMMENT 'Typed lifecycle events.'
AS SELECT
  event_id,
  customer_id,
  event_type,
  CAST(event_date AS DATE) AS event_date,
  NULLIF(source_bank, '') AS source_bank,
  reason
FROM bronze_customer_events;


-- ---------------------------------------------------------------------------
-- GOLD — business-ready. One customer-360 fact, one transaction fact, and the
-- pre-aggregated tables the dashboard and Genie agent lean on.
-- ---------------------------------------------------------------------------

-- gold_customer_360 — one row per customer: the table most questions resolve to.
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_360
  COMMENT 'One row per customer with balances, activity, tenure, and retention flags.'
AS
WITH bal AS (
  SELECT customer_id,
         COUNT(*) AS num_accounts,
         ROUND(SUM(balance), 2) AS total_balance,
         ROUND(AVG(balance), 2) AS avg_account_balance
  FROM silver_accounts GROUP BY customer_id
),
tx AS (
  SELECT a.customer_id,
         COUNT(*) AS txn_count,
         COUNT_IF(t.card_type = 'debit' AND t.channel = 'swipe') AS debit_swipe_count,
         COUNT_IF(t.card_type = 'debit') AS debit_txn_count,
         ROUND(SUM(t.amount), 2) AS total_spend
  FROM silver_card_transactions t
  JOIN silver_accounts a ON t.account_id = a.account_id
  GROUP BY a.customer_id
)
SELECT
  c.customer_id,
  c.age,
  c.age_band,
  c.gender,
  c.home_state,
  br.branch_region AS region,
  c.segment,
  c.acquisition_channel,
  c.source_bank,
  c.moved_from_bank,
  c.join_date,
  c.tenure_years,
  c.customer_status,
  c.is_retained,
  c.attrition_flag,
  COALESCE(bal.num_accounts, 0)         AS num_accounts,
  COALESCE(bal.total_balance, 0)        AS total_balance,
  COALESCE(bal.avg_account_balance, 0)  AS avg_account_balance,
  COALESCE(tx.txn_count, 0)             AS txn_count,
  COALESCE(tx.debit_swipe_count, 0)     AS debit_swipe_count,
  COALESCE(tx.debit_txn_count, 0)       AS debit_txn_count,
  COALESCE(tx.total_spend, 0)           AS total_spend
FROM silver_customers c
LEFT JOIN silver_branches br ON c.home_branch_id = br.branch_id
LEFT JOIN bal ON c.customer_id = bal.customer_id
LEFT JOIN tx  ON c.customer_id = tx.customer_id;

-- gold_transaction_facts — one row per transaction, enriched with customer age
-- band / segment / region for slicing (e.g. "debit swipes for ages 21-25").
CREATE OR REFRESH MATERIALIZED VIEW gold_transaction_facts
  COMMENT 'Transaction fact enriched with customer demographics for analysis.'
AS SELECT
  t.txn_id,
  t.txn_date,
  t.card_type,
  t.channel,
  t.merchant_category,
  t.amount,
  c.customer_id,
  c.age,
  c.age_band,
  c.segment,
  c.home_state,
  c.region
FROM silver_card_transactions t
JOIN silver_accounts a ON t.account_id = a.account_id
JOIN gold_customer_360 c ON a.customer_id = c.customer_id;

-- gold_balance_by_age_band — pre-aggregated balances for the dashboard.
CREATE OR REFRESH MATERIALIZED VIEW gold_balance_by_age_band
  COMMENT 'Customer counts and balances by age band.'
AS SELECT
  age_band,
  COUNT(*) AS customer_count,
  ROUND(AVG(total_balance), 2) AS avg_balance,
  ROUND(SUM(total_balance), 2) AS total_balance
FROM gold_customer_360
GROUP BY age_band;

-- gold_retention_summary — "who moved to our bank, and who stayed?"
CREATE OR REFRESH MATERIALIZED VIEW gold_retention_summary
  COMMENT 'Switch-in acquisition and retention, by source bank.'
AS SELECT
  COALESCE(source_bank, 'unknown') AS source_bank,
  COUNT(*) AS moved_in,
  COUNT_IF(is_retained) AS stayed,
  COUNT_IF(NOT is_retained) AS left_us,
  ROUND(COUNT_IF(is_retained) / COUNT(*), 4) AS retention_rate
FROM gold_customer_360
WHERE moved_from_bank
GROUP BY COALESCE(source_bank, 'unknown');
