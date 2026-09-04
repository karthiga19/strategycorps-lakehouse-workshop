# Databricks notebook source
# MAGIC %md
# MAGIC # Catch-up / answer key — the semantic layer
# MAGIC
# MAGIC Creates the `mv_banking_metrics` metric view over your `gold_customer_360`,
# MAGIC governs it with a comment + tags, and runs the three anchor-question
# MAGIC validation queries. This is the answer key for Leg 2.
# MAGIC
# MAGIC Requires `gold_customer_360` to exist — run Leg 1, `01_medallion`, or
# MAGIC `00_fast_catchup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog (ask your facilitator)")
dbutils.widgets.text("schema", "", "2. Schema (blank = your own)")

if not dbutils.widgets.get("catalog").strip():
    print("WAITING FOR INPUT — type the catalog into box 1, leave box 2 blank, Run all.")
    dbutils.notebook.exit("NEEDS_CATALOG")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# The metric-view YAML. `source` is filled in from your schema.
yaml_body = f"""
  version: 1.1
  source: {FQ}.gold_customer_360
  comment: "Governed retail-banking metrics: balances, card activity, and switch-in retention."
  dimensions:
    - name: Age
      expr: age
    - name: Age Band
      expr: age_band
    - name: Gender
      expr: gender
    - name: State
      expr: home_state
    - name: Region
      expr: region
    - name: Segment
      expr: segment
    - name: Acquisition Channel
      expr: acquisition_channel
    - name: Source Bank
      expr: source_bank
    - name: Moved From Bank
      expr: moved_from_bank
    - name: Customer Status
      expr: customer_status
    - name: Retained
      expr: is_retained
  measures:
    - name: Customer Count
      expr: COUNT(1)
    - name: Average Balance
      expr: AVG(total_balance)
    - name: Total Balance
      expr: SUM(total_balance)
    - name: Median Balance
      expr: PERCENTILE(total_balance, 0.5)
    - name: Debit Card Swipes
      expr: SUM(debit_swipe_count)
    - name: Total Transactions
      expr: SUM(txn_count)
    - name: Total Spend
      expr: SUM(total_spend)
    - name: Retained Customers
      expr: SUM(CASE WHEN is_retained THEN 1 ELSE 0 END)
    - name: Retention Rate
      expr: AVG(CASE WHEN is_retained THEN 1.0 ELSE 0.0 END)
    - name: Attrition Rate
      expr: AVG(CASE WHEN customer_status = 'attrited' THEN 1.0 ELSE 0.0 END)
"""

spark.sql(f"""
CREATE OR REPLACE VIEW {METRIC_VIEW}
WITH METRICS LANGUAGE YAML AS $${yaml_body}$$
""")
print(f"created metric view {METRIC_VIEW}")

# COMMAND ----------

# Govern it: discoverability tags. (COMMENT is already set inside the YAML.)
try:
    spark.sql(f"ALTER VIEW {METRIC_VIEW} SET TAGS ('domain' = 'retail_banking', 'certified' = 'true')")
    print("applied tags: domain=retail_banking, certified=true")
except Exception as e:
    print(f"(tagging skipped: {str(e).splitlines()[0][:120]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three anchor questions, through the metric view

# COMMAND ----------

print("Q1 — average balance, ages 61-65:")
display(spark.sql(f"""
SELECT MEASURE(`Average Balance`) AS avg_balance, MEASURE(`Customer Count`) AS n
FROM {METRIC_VIEW} WHERE `Age` BETWEEN 61 AND 65
"""))

print("Q2 — debit card swipes, ages 21-25:")
display(spark.sql(f"""
SELECT MEASURE(`Debit Card Swipes`) AS debit_swipes
FROM {METRIC_VIEW} WHERE `Age` BETWEEN 21 AND 25
"""))

print("Q3 — moved to our bank, and retention:")
display(spark.sql(f"""
SELECT MEASURE(`Customer Count`) AS moved_in, MEASURE(`Retention Rate`) AS retention_rate
FROM {METRIC_VIEW} WHERE `Moved From Bank` = true
"""))

# COMMAND ----------

print("✅ Semantic layer built. Ready for Leg 3 (Dashboard) and Leg 4 (Genie).")
dbutils.notebook.exit("SUCCESS")
