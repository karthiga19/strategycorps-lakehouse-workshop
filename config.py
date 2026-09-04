# Databricks notebook source
# MAGIC %md
# MAGIC # `config` — shared plumbing
# MAGIC
# MAGIC **You do not need to open or run this notebook directly.** Every other
# MAGIC notebook pulls it in with `%run ../config`, which is what creates the two
# MAGIC input boxes you see at the top and works out your catalog, schema, volume,
# MAGIC pipeline, metric-view and experiment names.
# MAGIC
# MAGIC To set up for the workshop, open **`setup/00_setup`** instead.
# MAGIC
# MAGIC | Widget | What to put in it |
# MAGIC |---|---|
# MAGIC | **catalog** | The workshop catalog. Your facilitator gives you this on the day. |
# MAGIC | **schema** | Leave blank to get your own private schema, derived from your username. |
# MAGIC
# MAGIC The widgets are per-notebook, so you set them once in each notebook you open.

# COMMAND ----------

# Idempotent: re-declaring a widget does not clear a value the user already
# typed, so it is safe that the calling notebook declares these too. Defaults are
# intentionally empty for catalog, so nobody silently builds tables in the wrong
# place.
dbutils.widgets.text("catalog", "", "1. Catalog (ask your facilitator)")
dbutils.widgets.text("schema", "", "2. Schema (blank = your own)")

# COMMAND ----------

import re
import zlib

USER = spark.sql("SELECT current_user()").first()[0]

# Sanitize the email local-part into something legal as a schema name.
HANDLE = re.sub(r"[^a-z0-9]+", "_", USER.split("@")[0].lower()).strip("_")

CATALOG = dbutils.widgets.get("catalog").strip()
_schema_input = dbutils.widgets.get("schema").strip()

# Per-user schema by default: everyone in one shared catalog would otherwise
# overwrite each other's tables.
SCHEMA_NAME = _schema_input or f"sc_{HANDLE}"

VOLUME_NAME = "landing"

# COMMAND ----------

# Safety net only. Each notebook declares these widgets and stops cleanly in its
# own first cell if the catalog is blank -- see the "gate" cell in setup/00_setup.
if not CATALOG:
    raise ValueError(
        "\n\n  No catalog set. The notebook that called config is missing its"
        "\n  widget gate cell — copy the first cell from setup/00_setup.\n"
    )

if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    raise ValueError(
        f"\n\n  Catalog name '{CATALOG}' looks wrong — letters, digits and "
        f"underscores only.\n  Do not include the schema; there is a separate "
        f"box for that.\n"
    )

if not re.fullmatch(r"[A-Za-z0-9_]+", SCHEMA_NAME):
    raise ValueError(
        f"\n\n  Schema name '{SCHEMA_NAME}' looks wrong — letters, digits and "
        f"underscores only.\n"
    )

# COMMAND ----------

FQ = f"{CATALOG}.{SCHEMA_NAME}"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA_NAME}/{VOLUME_NAME}"

# The DLT / Lakeflow Declarative Pipeline you build in Leg 2 writes into this
# same schema; naming it here keeps the pipeline definition and the catch-up
# notebooks pointing at one place.
PIPELINE_NAME = f"sc_medallion_{HANDLE}"

# Semantic layer (Leg 3) and optional churn model (free play).
METRIC_VIEW = f"{FQ}.mv_banking_metrics"
MODEL_NAME = f"{FQ}.attrition_clf"

# Top-level /Shared path: an MLflow experiment under a not-yet-existing /Users
# subfolder fails silently. Suffixed with the handle so concurrent participants
# get separate experiments.
EXPERIMENT_PATH = f"/Shared/sc_{HANDLE}_attrition"

# Read-only schema the facilitator pre-builds, holding correct output for every
# leg. The catch-up notebooks fall back to building from scratch if it is absent.
CHECKPOINT_SCHEMA = f"{CATALOG}.workshop_checkpoints"

# Deterministic per-user seed. zlib.crc32 rather than hash(), because hash() is
# salted per process and would give a different dataset on every run.
DATA_SEED = zlib.crc32(HANDLE.encode()) % (2**31)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{VOLUME_NAME}")

print(f"user        : {USER}")
print(f"catalog     : {CATALOG}")
print(f"schema      : {FQ}")
print(f"volume      : {LANDING}")
print(f"pipeline    : {PIPELINE_NAME}")
print(f"metric view : {METRIC_VIEW}")
print(f"model       : {MODEL_NAME}")
print(f"experiment  : {EXPERIMENT_PATH}")
print(f"checkpoints : {CHECKPOINT_SCHEMA}")
print(f"seed        : {DATA_SEED}")
print()
print("^ check the schema line above is yours before you build anything.")
