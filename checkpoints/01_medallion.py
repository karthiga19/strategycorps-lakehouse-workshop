# Databricks notebook source
# MAGIC %md
# MAGIC # Catch-up / answer key — the medallion in plain SQL
# MAGIC
# MAGIC Builds bronze → silver → gold from your landing files as regular Delta
# MAGIC tables, about a minute. This is the **answer key** for Leg 1 — the same
# MAGIC logic the DLT pipeline expresses declaratively, written out so you can read
# MAGIC it. Use it to catch up, or to compare against your pipeline's output.
# MAGIC
# MAGIC The one thing it does *not* show is the pipeline machinery itself — Auto
# MAGIC Loader, expectations as first-class DQ metrics, the lineage graph. For that,
# MAGIC build the real pipeline (`pipeline/medallion_dlt.sql`).

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog (ask your facilitator)")
dbutils.widgets.text("schema", "", "2. Schema (blank = your own)")

if not dbutils.widgets.get("catalog").strip():
    print("WAITING FOR INPUT — type the catalog into box 1, leave box 2 blank, Run all.")
    dbutils.notebook.exit("NEEDS_CATALOG")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql import functions as F


def run(sql):
    spark.sql(sql)


def count(t):
    n = spark.table(f"{FQ}.{t}").count()
    print(f"  {t:<32} {n:>8,} rows")
    return n


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — raw, as-landed

# COMMAND ----------

csv_opts = {"header": "true", "inferSchema": "false"}

# Each source lands in its own subdirectory; read the directory (as Auto Loader
# does in the DLT version).
for name in ["customers", "accounts", "branches", "customer_events"]:
    (spark.read.options(**csv_opts).csv(f"{LANDING}/{name}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{FQ}.bronze_{name}"))

(spark.read.json(f"{LANDING}/card_transactions/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{FQ}.bronze_card_transactions"))

print("bronze:")
for t in ["bronze_customers", "bronze_accounts", "bronze_branches",
          "bronze_customer_events", "bronze_card_transactions"]:
    count(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — typed, deduped, quarantined

# COMMAND ----------

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_branches AS
SELECT branch_id, branch_name, branch_state, branch_region,
       CAST(opened_date AS DATE) AS opened_date
FROM {FQ}.bronze_branches
""")

# Deduped + parsed customers, reused by both the clean and quarantine tables.
run(f"""
CREATE OR REPLACE TEMP VIEW _cust_parsed AS
WITH deduped AS (
  SELECT * FROM {FQ}.bronze_customers
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY CAST(updated_at AS DATE) DESC) = 1
)
SELECT
  customer_id, first_name, last_name,
  COALESCE(TRY_TO_DATE(date_of_birth), TRY_TO_DATE(date_of_birth,'MM/dd/yyyy')) AS date_of_birth,
  date_of_birth AS dob_raw, gender,
  CASE UPPER(home_state)
    WHEN 'TENNESSEE' THEN 'TN' WHEN 'TEXAS' THEN 'TX' WHEN 'GEORGIA' THEN 'GA'
    WHEN 'FLORIDA' THEN 'FL' WHEN 'NORTH CAROLINA' THEN 'NC' WHEN 'OHIO' THEN 'OH'
    WHEN 'ILLINOIS' THEN 'IL' WHEN 'MICHIGAN' THEN 'MI' WHEN 'CALIFORNIA' THEN 'CA'
    WHEN 'ARIZONA' THEN 'AZ' WHEN 'NEW YORK' THEN 'NY' WHEN 'PENNSYLVANIA' THEN 'PA'
    ELSE UPPER(home_state) END AS home_state,
  home_zip, home_branch_id, segment, acquisition_channel,
  NULLIF(source_bank,'') AS source_bank, CAST(join_date AS DATE) AS join_date,
  customer_status, CAST(attrition_flag AS INT) AS attrition_flag,
  CAST(updated_at AS DATE) AS updated_at
FROM deduped
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_customers AS
SELECT * EXCEPT(dob_raw, age0),
  age0 AS age,
  CASE
    WHEN age0 BETWEEN 18 AND 25 THEN '18-25' WHEN age0 BETWEEN 26 AND 35 THEN '26-35'
    WHEN age0 BETWEEN 36 AND 45 THEN '36-45' WHEN age0 BETWEEN 46 AND 55 THEN '46-55'
    WHEN age0 BETWEEN 56 AND 65 THEN '56-65' WHEN age0 BETWEEN 66 AND 75 THEN '66-75'
    ELSE '76+' END AS age_band,
  ROUND(DATEDIFF(current_date(), join_date)/365.25, 2) AS tenure_years,
  (acquisition_channel = 'switch_kit') AS moved_from_bank,
  (customer_status <> 'attrited') AS is_retained
FROM (
  SELECT *, FLOOR(DATEDIFF(current_date(), date_of_birth)/365.25) AS age0 FROM _cust_parsed
)
WHERE date_of_birth IS NOT NULL AND age0 BETWEEN 18 AND 110
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_customers_quarantine AS
SELECT customer_id, dob_raw,
  CASE WHEN date_of_birth IS NULL THEN 'missing_or_unparseable_dob'
       ELSE 'implausible_age' END AS quarantine_reason
FROM (SELECT *, FLOOR(DATEDIFF(current_date(), date_of_birth)/365.25) AS age0 FROM _cust_parsed)
WHERE date_of_birth IS NULL OR age0 NOT BETWEEN 18 AND 110
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_accounts AS
SELECT a.account_id, a.customer_id, a.account_type, CAST(a.open_date AS DATE) AS open_date,
       a.account_status, CAST(a.balance AS DOUBLE) AS balance
FROM {FQ}.bronze_accounts a
SEMI JOIN {FQ}.silver_customers c ON a.customer_id = c.customer_id
WHERE CAST(a.balance AS DOUBLE) >= 0 AND CAST(a.balance AS DOUBLE) < 10000000
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_accounts_quarantine AS
SELECT a.account_id, a.customer_id, CAST(a.balance AS DOUBLE) AS balance,
  CASE WHEN c.customer_id IS NULL THEN 'orphan_customer_id'
       WHEN CAST(a.balance AS DOUBLE) < 0 THEN 'negative_balance'
       WHEN CAST(a.balance AS DOUBLE) >= 10000000 THEN 'balance_outlier_100x' END AS quarantine_reason
FROM {FQ}.bronze_accounts a
LEFT JOIN {FQ}.silver_customers c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL OR CAST(a.balance AS DOUBLE) < 0 OR CAST(a.balance AS DOUBLE) >= 10000000
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_card_transactions AS
SELECT txn_id, account_id, card_type, channel, merchant_category,
       CAST(amount AS DOUBLE) AS amount, CAST(txn_ts AS DATE) AS txn_date
FROM {FQ}.bronze_card_transactions
WHERE CAST(amount AS DOUBLE) < 100000
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_ts) = 1
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.silver_customer_events AS
SELECT event_id, customer_id, event_type, CAST(event_date AS DATE) AS event_date,
       NULLIF(source_bank,'') AS source_bank, reason
FROM {FQ}.bronze_customer_events
""")

print("silver:")
for t in ["silver_customers", "silver_customers_quarantine", "silver_accounts",
          "silver_accounts_quarantine", "silver_card_transactions",
          "silver_customer_events", "silver_branches"]:
    count(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — business-ready

# COMMAND ----------

run(f"""
CREATE OR REPLACE TABLE {FQ}.gold_customer_360 AS
WITH bal AS (
  SELECT customer_id, COUNT(*) AS num_accounts,
         ROUND(SUM(balance),2) AS total_balance, ROUND(AVG(balance),2) AS avg_account_balance
  FROM {FQ}.silver_accounts GROUP BY customer_id
),
tx AS (
  SELECT a.customer_id, COUNT(*) AS txn_count,
         COUNT_IF(t.card_type='debit' AND t.channel='swipe') AS debit_swipe_count,
         COUNT_IF(t.card_type='debit') AS debit_txn_count, ROUND(SUM(t.amount),2) AS total_spend
  FROM {FQ}.silver_card_transactions t
  JOIN {FQ}.silver_accounts a ON t.account_id = a.account_id
  GROUP BY a.customer_id
)
SELECT c.customer_id, c.age, c.age_band, c.gender, c.home_state, br.branch_region AS region,
       c.segment, c.acquisition_channel, c.source_bank, c.moved_from_bank, c.join_date,
       c.tenure_years, c.customer_status, c.is_retained, c.attrition_flag,
       COALESCE(bal.num_accounts,0) AS num_accounts,
       COALESCE(bal.total_balance,0) AS total_balance,
       COALESCE(bal.avg_account_balance,0) AS avg_account_balance,
       COALESCE(tx.txn_count,0) AS txn_count,
       COALESCE(tx.debit_swipe_count,0) AS debit_swipe_count,
       COALESCE(tx.debit_txn_count,0) AS debit_txn_count,
       COALESCE(tx.total_spend,0) AS total_spend
FROM {FQ}.silver_customers c
LEFT JOIN {FQ}.silver_branches br ON c.home_branch_id = br.branch_id
LEFT JOIN bal ON c.customer_id = bal.customer_id
LEFT JOIN tx  ON c.customer_id = tx.customer_id
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.gold_transaction_facts AS
SELECT t.txn_id, t.txn_date, t.card_type, t.channel, t.merchant_category, t.amount,
       c.customer_id, c.age, c.age_band, c.segment, c.home_state, c.region
FROM {FQ}.silver_card_transactions t
JOIN {FQ}.silver_accounts a ON t.account_id = a.account_id
JOIN {FQ}.gold_customer_360 c ON a.customer_id = c.customer_id
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.gold_balance_by_age_band AS
SELECT age_band, COUNT(*) AS customer_count,
       ROUND(AVG(total_balance),2) AS avg_balance, ROUND(SUM(total_balance),2) AS total_balance
FROM {FQ}.gold_customer_360 GROUP BY age_band
""")

run(f"""
CREATE OR REPLACE TABLE {FQ}.gold_retention_summary AS
SELECT COALESCE(source_bank,'unknown') AS source_bank, COUNT(*) AS moved_in,
       COUNT_IF(is_retained) AS stayed, COUNT_IF(NOT is_retained) AS left_us,
       ROUND(COUNT_IF(is_retained)/COUNT(*),4) AS retention_rate
FROM {FQ}.gold_customer_360 WHERE moved_from_bank GROUP BY COALESCE(source_bank,'unknown')
""")

print("gold:")
for t in ["gold_customer_360", "gold_transaction_facts",
          "gold_balance_by_age_band", "gold_retention_summary"]:
    count(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity — the three anchor questions

# COMMAND ----------

display(spark.sql(f"""
SELECT ROUND(AVG(total_balance),0) AS avg_balance_61_65
FROM {FQ}.gold_customer_360 WHERE age BETWEEN 61 AND 65
"""))
display(spark.sql(f"""
SELECT COUNT(*) AS debit_swipes_21_25 FROM {FQ}.gold_transaction_facts
WHERE age BETWEEN 21 AND 25 AND card_type='debit' AND channel='swipe'
"""))
display(spark.sql(f"""
SELECT COUNT(*) AS moved_in, COUNT_IF(is_retained) AS stayed,
       ROUND(COUNT_IF(is_retained)/COUNT(*),3) AS retention_rate
FROM {FQ}.gold_customer_360 WHERE moved_from_bank
"""))

# COMMAND ----------

print("✅ Medallion rebuilt. Ready for Leg 2 (Semantic layer).")
dbutils.notebook.exit("SUCCESS")
