# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop setup — run this one notebook, top to bottom
# MAGIC
# MAGIC This is the **only** setup notebook. It does three things in order:
# MAGIC
# MAGIC 1. **Checks your environment** and tells you specifically what is wrong if
# MAGIC    anything is.
# MAGIC 2. **Generates** synthetic retail-banking data into your Volume as raw
# MAGIC    CSV + JSON files — the *landing zone* your pipeline will ingest.
# MAGIC 3. **Previews** the raw files so you can see the deliberate mess before the
# MAGIC    session.
# MAGIC
# MAGIC Unlike a notebook-only workshop, we do **not** pre-build any tables here.
# MAGIC Building the medallion (bronze → silver → gold) is Leg 2, and you do it with
# MAGIC a **Lakeflow Declarative Pipeline (DLT)** that reads these files.
# MAGIC
# MAGIC ### Before you run
# MAGIC
# MAGIC Two input boxes appear at the top of this notebook once you run the first
# MAGIC cell:
# MAGIC
# MAGIC | Box | What to enter |
# MAGIC |---|---|
# MAGIC | **1. Catalog** | The workshop catalog name — your facilitator provides it |
# MAGIC | **2. Schema** | Leave blank. You get your own private schema automatically |
# MAGIC
# MAGIC Then **Run all**. Takes two to three minutes.
# MAGIC
# MAGIC ### 📸 At the end
# MAGIC
# MAGIC Screenshot the final summary and reply to the workshop invite with it. That
# MAGIC is how we confirm you are ready.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — fill in the catalog
# MAGIC
# MAGIC Run the cell below. It creates the two input boxes at the top of the
# MAGIC notebook and then stops, because on a first run there is nothing in them
# MAGIC yet. Type the catalog name into box 1, leave box 2 blank, then **Run all**.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog (ask your facilitator)")
dbutils.widgets.text("schema", "", "2. Schema (blank = your own)")

# COMMAND ----------

# The widgets are declared HERE, in the calling notebook, rather than only inside
# config. On the first run of a fresh notebook the boxes do not exist until this
# cell has executed, so anything that reads them in the same pass would see an
# empty value no matter what the user does.
if not dbutils.widgets.get("catalog").strip():
    print("=" * 72)
    print("  WAITING FOR INPUT — this is not an error.")
    print("=" * 72)
    print()
    print("  Look at the top of this notebook. There are now two input boxes:")
    print()
    print("     1. Catalog  <- type the workshop catalog name here")
    print("     2. Schema   <- leave blank to get your own private schema")
    print()
    print("  Your facilitator provides the catalog name — it is in the workshop")
    print("  invite, and will be on the whiteboard on the day.")
    print()
    print("  Fill in box 1, then use Run all.")
    print("=" * 72)
    dbutils.notebook.exit("NEEDS_CATALOG")

print(f"catalog set to '{dbutils.widgets.get('catalog').strip()}' — continuing.")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1 · Environment checks
# MAGIC
# MAGIC Every check is recorded rather than raised, so you get the full picture in
# MAGIC one pass instead of fixing one error at a time.

# COMMAND ----------

CHECKS = []


def check(label, fn, required=True, hint=""):
    """Run one check, record pass/warn/fail, never raise."""
    try:
        detail = fn()
        CHECKS.append(("PASS", label, detail or ""))
    except Exception as e:
        msg = str(e).split("\n")[0][:180]
        CHECKS.append(("FAIL" if required else "WARN", label, f"{msg}  {hint}".strip()))


# COMMAND ----------

check("Notebook compute attached and Spark responding",
      lambda: spark.sql("SELECT current_user()").first()[0])

check("Compute identified", required=False, fn=lambda: "compute: " + spark.conf.get(
    "spark.databricks.clusterUsageTags.clusterName", "serverless (or unknown)"))

check("Config resolved (catalog, schema, seed)",
      lambda: f"{FQ}  (seed {DATA_SEED})",
      hint="-> fill in the 'catalog' box at the top of this notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog access
# MAGIC
# MAGIC The most common failure. You need `USE CATALOG` on the workshop catalog and
# MAGIC `CREATE SCHEMA` within it — a workspace admin grants both.

# COMMAND ----------

def _catalog():
    spark.sql(f"DESCRIBE CATALOG {CATALOG}")
    return f"catalog '{CATALOG}' reachable"


check("Catalog reachable", _catalog,
      hint="-> check the spelling first; if right, ask your facilitator "
           "(you need USE CATALOG on it).")


def _schema():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    return FQ


check("Your schema exists", _schema,
      hint=f"-> you need CREATE SCHEMA on catalog '{CATALOG}'.")


def _volume():
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{VOLUME_NAME}")
    dbutils.fs.ls(LANDING)
    return LANDING


check("Your Volume exists and is listable", _volume,
      hint="-> you need CREATE VOLUME on your schema.")


def _write_read():
    p = f"{LANDING}/_setup_probe.txt"
    dbutils.fs.put(p, "ok", overwrite=True)
    content = dbutils.fs.head(p)
    dbutils.fs.rm(p)
    if content.strip() != "ok":
        raise RuntimeError("readback mismatch")
    return "wrote and read back a test file"


check("Volume is writable", _write_read)


def _table():
    t = f"{FQ}._setup_probe"
    spark.sql(f"CREATE OR REPLACE TABLE {t} (a INT)")
    spark.sql(f"INSERT INTO {t} VALUES (1)")
    n = spark.table(t).count()
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    if n != 1:
        raise RuntimeError(f"expected 1 row, got {n}")
    return "created, inserted, dropped a Delta table"


check("Delta table create / insert / drop", _table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The data generator

# COMMAND ----------

import os
import sys

_generator = None


def _generator_import():
    global _generator
    here = os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        .notebookPath().get()
    )
    sys.path.insert(0, f"/Workspace{here}")
    import _generator as _g
    if not hasattr(_g, "generate"):
        raise RuntimeError("_generator has no generate()")
    _generator = _g
    return "_generator.py found and importable"


check("Data generator importable", _generator_import,
      hint="-> make sure you cloned the whole repo, not just this notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional — warnings here will not block you
# MAGIC
# MAGIC The shared checkpoint schema is a convenience for catching up mid-session.
# MAGIC MLflow / scikit-learn power the optional churn free-play leg. Genie Code
# MAGIC skills make every leg go more smoothly.

# COMMAND ----------

def _checkpoints():
    n = spark.sql(f"SHOW TABLES IN {CHECKPOINT_SCHEMA}").count()
    if n == 0:
        raise RuntimeError(f"{CHECKPOINT_SCHEMA} exists but is empty")
    return f"{n} checkpoint tables available"


check("Shared checkpoint schema readable", _checkpoints, required=False,
      hint="-> optional; the catch-up notebooks build from scratch instead.")


def _mlflow():
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    return f"mlflow {mlflow.__version__} (only needed for the optional churn leg)"


check("MLflow importable, UC registry reachable", _mlflow, required=False,
      hint="-> optional; only the churn free-play leg uses it.")


def _ai_functions():
    r = spark.sql(
        "SELECT ai_classify('closed my account over monthly fees', "
        "array('fees','rate','service')) AS c"
    ).first()["c"]
    return f"ai_classify returned '{r}'"


check("AI functions (ai_classify) available", _ai_functions, required=False,
      hint="-> optional; affects one free-play exercise only.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Code skills — installed automatically
# MAGIC
# MAGIC Every leg goes noticeably better with the skills loaded. This cell installs
# MAGIC them from the cloned repo into `~/.assistant/skills/` and verifies they
# MAGIC landed. Already installed? It refreshes them.

# COMMAND ----------

import shutil

SKILLS_DEST = f"/Workspace/Users/{USER}/.assistant/skills"

_nb_dir = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get()
)

REPO_ROOT = None
_probe = _nb_dir
for _ in range(4):
    _probe = os.path.dirname(_probe)
    if os.path.isdir(os.path.join(_probe, ".assistant", "skills")):
        REPO_ROOT = _probe
        break

SKILLS_SRC = os.path.join(REPO_ROOT, ".assistant", "skills") if REPO_ROOT else None

INSTALLED = []


def _install_skills():
    if SKILLS_SRC is None:
        raise RuntimeError(
            f"could not find .assistant/skills above {_nb_dir} — "
            f"did you clone the whole repo?"
        )
    names = sorted(
        d for d in os.listdir(SKILLS_SRC)
        if os.path.isfile(os.path.join(SKILLS_SRC, d, "SKILL.md"))
    )
    if not names:
        raise RuntimeError(f"{SKILLS_SRC} contains no skill folders")
    os.makedirs(SKILLS_DEST, exist_ok=True)
    for n in names:
        src = os.path.join(SKILLS_SRC, n)
        dst = os.path.join(SKILLS_DEST, n)
        if os.path.isdir(dst):
            shutil.rmtree(dst)          # refresh rather than merge
        shutil.copytree(src, dst)
        INSTALLED.append(n)
    landed = [
        os.path.basename(dp) for dp, _, fn in os.walk(SKILLS_DEST)
        if "SKILL.md" in fn
    ]
    missing = [n for n in names if n not in landed]
    if missing:
        raise RuntimeError(f"copied but not found on readback: {missing}")
    return f"installed {len(names)}: {', '.join(names)}"


check("Genie Code skills installed", _install_skills, required=False,
      hint="-> not fatal; you can still do every leg, but prompts may need "
           "more correcting.")

if INSTALLED:
    print(f"\n  Skills installed to: {SKILLS_DEST}")
    for n in INSTALLED:
        print(f"    - {n}")
    print("\n  ⚠️  IMPORTANT: start a NEW Genie Code chat thread before the session.")
    print("     Skills are discovered when a thread opens, so an already-open")
    print("     thread will not see them. Hard-refresh the page if they do not")
    print("     appear in a new thread.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint results
# MAGIC
# MAGIC If anything failed, this cell stops the notebook **before** it creates any
# MAGIC data, and lists what to fix.

# COMMAND ----------

ICON = {"PASS": "[ PASS ]", "WARN": "[ WARN ]", "FAIL": "[ FAIL ]"}

n_pass = sum(1 for s, _, _ in CHECKS if s == "PASS")
n_warn = sum(1 for s, _, _ in CHECKS if s == "WARN")
n_fail = sum(1 for s, _, _ in CHECKS if s == "FAIL")

print("=" * 76)
print("  PART 1 — ENVIRONMENT CHECKS")
print("=" * 76)
for status, label, detail in CHECKS:
    print(f"{ICON[status]}  {label}")
    if detail:
        print(f"           {detail}")
print("=" * 76)
print(f"  {n_pass} passed, {n_warn} warning(s), {n_fail} failure(s)")
print("=" * 76)

if n_fail:
    raise RuntimeError(
        f"\n\n  {n_fail} environment check(s) failed — see the [ FAIL ] lines above."
        f"\n  Nothing has been created. Fix those, then Run all again."
        f"\n\n  Stuck? Bring this output to the pre-session office hours.\n"
    )

print("\n  Environment is good. Continuing to data generation...")
if n_warn:
    print("  (The warnings above are optional features and will not block you.)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 · Generate the landing data
# MAGIC
# MAGIC Writes CSV and JSON into your Volume — the raw landing zone your Leg-2
# MAGIC pipeline ingests. The generator uses only the Python standard library, so
# MAGIC there is no `%pip install` that can fail.
# MAGIC
# MAGIC `DATA_SEED` derives from your username, so your dataset is stable across
# MAGIC re-runs but slightly different from your neighbour's.

# COMMAND ----------

# Idempotent: clear any previous run so re-running never doubles the data.
# Files land in per-entity subdirectories, so remove recursively.
try:
    existing = dbutils.fs.ls(LANDING)
    for f in existing:
        dbutils.fs.rm(f.path, recurse=True)
    if existing:
        print(f"cleared {len(existing)} existing item(s) from {LANDING}")
except Exception:
    pass

counts = _generator.generate(DATA_SEED, LANDING)

print()
print("=" * 60)
print(f"  seed               : {DATA_SEED}")
print(f"  branches           : {counts['branches']:>7,} rows")
print(f"  customers          : {counts['customers']:>7,} rows  (with duplicates)")
print(f"  accounts           : {counts['accounts']:>7,} rows")
print(f"  card_transactions  : {counts['card_transactions']:>7,} rows  "
      f"({counts['transaction_shards']} JSON files)")
print(f"  customer_events    : {counts['customer_events']:>7,} rows")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planted data-quality defects
# MAGIC
# MAGIC This data is deliberately dirty. During the DLT leg you will catch each of
# MAGIC these with a pipeline **expectation** (quarantine) or a dedup rule. The
# MAGIC counts below are your answer key — keep them.

# COMMAND ----------

d = counts["defects"]
print(f"  duplicate customer rows      : {d['duplicate_customer_rows']:>4}   "
      f"(keep the latest updated_at)")
print(f"  duplicate transaction rows   : {d['duplicate_txn_rows']:>4}")
print(f"  long-form state names        : {d['long_form_state']:>4}   "
      f"('Tennessee' instead of 'TN')")
print(f"  alternate dob date format    : {d['alt_date_format_dob']:>4}   "
      f"(MM/DD/YYYY instead of ISO)")
print(f"  missing date_of_birth        : {d['missing_dob']:>4}")
print(f"  negative account balance     : {d['negative_balance']:>4}")
print(f"  100x balance typos           : {d['balance_outlier_100x']:>4}")
print(f"  orphan account customer_id   : {d['orphan_account_customer']:>4}")
print(f"  100x transaction amount typos: {d['txn_amount_outlier_100x']:>4}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3 · A first look at your raw files
# MAGIC
# MAGIC No tables yet — that is the point. These are the raw files exactly as they
# MAGIC landed, strings and all. Reading them here is the "look before you build"
# MAGIC moment for the medallion intro.

# COMMAND ----------

print("source folders landed in your Volume (one per entity):\n")
for d in dbutils.fs.ls(LANDING):
    files = [f for f in dbutils.fs.ls(d.path)]
    total = sum(f.size for f in files)
    print(f"  {d.name:<22} {len(files)} file(s)  {total:>10,} bytes")

# COMMAND ----------

# CSV lands as all-strings, which is what a raw ingest really looks like.
# Auto Loader reads a directory, so we point spark.read at the folder too.
display(
    spark.read.option("header", "true").option("inferSchema", "false")
    .csv(f"{LANDING}/customers/")
    .select("customer_id", "date_of_birth", "home_state", "segment",
            "acquisition_channel", "source_bank", "customer_status", "updated_at")
    .limit(15)
)

# COMMAND ----------

display(spark.read.json(f"{LANDING}/card_transactions/").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ Setup complete

# COMMAND ----------

expected = {"customers", "accounts", "branches", "customer_events", "card_transactions"}
present = {f.name.rstrip("/") for f in dbutils.fs.ls(LANDING)}
shards = [f.name for f in dbutils.fs.ls(f"{LANDING}/card_transactions/")] \
    if "card_transactions" in present else []

print("=" * 70)
print("  STRATEGY CORPS LAKEHOUSE WORKSHOP — SETUP COMPLETE")
print("=" * 70)
print(f"  user     : {USER}")
print(f"  schema   : {FQ}")
print(f"  volume   : {LANDING}")
print(f"  checks   : {n_pass} passed, {n_warn} warning(s), 0 failures")
print(f"  sources  : {len(present)} folders landed "
      f"({len(shards)} transaction shards)")
print("=" * 70)

if expected.issubset(present) and shards:
    print("\n  ✅ READY FOR THE WORKSHOP. Nothing else to do.")
    print("     You build the medallion live in the session — please don't run ahead.")
else:
    missing = expected - present
    print(f"\n  ⚠️  Missing expected source folders: {missing or 'transaction shards'}. "
          f"Re-run this notebook.")

# COMMAND ----------

# Exit signal, so a parent notebook orchestrating this one (see
# facilitator/build_checkpoints) gets an unambiguous success result.
dbutils.notebook.exit("SUCCESS")
