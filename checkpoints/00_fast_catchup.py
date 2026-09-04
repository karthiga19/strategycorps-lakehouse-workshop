# Databricks notebook source
# MAGIC %md
# MAGIC # Fast catch-up
# MAGIC
# MAGIC Copies finished tables from the shared checkpoint schema straight into your
# MAGIC own schema. **Seconds, not minutes.** Use this when you are behind and the
# MAGIC group is moving on.
# MAGIC
# MAGIC Pick how far to catch up from the **catch_up_to** dropdown, then run all.
# MAGIC
# MAGIC | Choose | You get | Ready for |
# MAGIC |---|---|---|
# MAGIC | `medallion` | bronze + silver + gold tables | Leg 2 (Semantic) |
# MAGIC | `scored` | + gold_customers_scored | churn free play |
# MAGIC
# MAGIC > This copies the medallion **tables**. It does not create the metric view
# MAGIC > (Leg 2) — that is one quick statement in `02_semantic`. Each level
# MAGIC > includes everything before it.

# COMMAND ----------

dbutils.widgets.dropdown("catch_up_to", "medallion", ["medallion", "scored"], "Catch up to")

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog (ask your facilitator)")
dbutils.widgets.text("schema", "", "2. Schema (blank = your own)")

if not dbutils.widgets.get("catalog").strip():
    print("=" * 72)
    print("  WAITING FOR INPUT -- this is not an error.")
    print("=" * 72)
    print("\n  Type the workshop catalog into box 1, leave box 2 blank, Run all.")
    dbutils.notebook.exit("NEEDS_CATALOG")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

LEVELS = {
    "medallion": [
        "bronze_customers", "bronze_accounts", "bronze_branches",
        "bronze_customer_events", "bronze_card_transactions",
        "silver_customers", "silver_customers_quarantine",
        "silver_accounts", "silver_accounts_quarantine",
        "silver_card_transactions", "silver_customer_events", "silver_branches",
        "gold_customer_360", "gold_transaction_facts",
        "gold_balance_by_age_band", "gold_retention_summary",
    ],
    "scored": ["gold_customers_scored"],
}
ORDER = ["medallion", "scored"]

target = dbutils.widgets.get("catch_up_to")
wanted = []
for level in ORDER:
    wanted.extend(LEVELS[level])
    if level == target:
        break

print(f"catching up to : {target}")
print(f"tables to copy : {len(wanted)}")
print(f"source         : {CHECKPOINT_SCHEMA}")
print(f"destination    : {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy
# MAGIC
# MAGIC `CREATE TABLE AS SELECT` rather than `DEEP CLONE`: CTAS needs only `SELECT`
# MAGIC on the source, so this works with plain read access to the shared schema.

# COMMAND ----------

try:
    available = {r.tableName for r in
                 spark.sql(f"SHOW TABLES IN {CHECKPOINT_SCHEMA}").collect()}
except Exception as e:
    raise RuntimeError(
        f"\n\n  Cannot read {CHECKPOINT_SCHEMA}."
        f"\n  {str(e).splitlines()[0][:160]}"
        f"\n\n  Tell your facilitator. Meanwhile use 01_medallion, which builds"
        f"\n  everything from your own landing files.\n"
    )

missing = [t for t in wanted if t not in available]
if missing:
    raise RuntimeError(
        f"\n\n  The shared checkpoint schema is missing: {missing}"
        f"\n  Tell your facilitator — it may not be fully built yet.\n"
    )

copied = []
for t in wanted:
    spark.sql(f"CREATE OR REPLACE TABLE {FQ}.{t} AS "
              f"SELECT * FROM {CHECKPOINT_SCHEMA}.{t}")
    n = spark.table(f"{FQ}.{t}").count()
    copied.append((t, n))
    print(f"  {t:<32} {n:>8,} rows")

# COMMAND ----------

print("=" * 60)
print(f"  Copied {len(copied)} tables. You are caught up to: {target}")
nxt = {"medallion": "Leg 2 — Semantic layer", "scored": "the churn free play"}[target]
print(f"  ✅ Ready for {nxt}. Rejoin the group.")
print("=" * 60)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {FQ}"))
