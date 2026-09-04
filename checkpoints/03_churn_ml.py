# Databricks notebook source
# MAGIC %md
# MAGIC # Optional — churn model (free-play answer key)
# MAGIC
# MAGIC Trains a GradientBoosting attrition model on `gold_customer_360`, registers
# MAGIC it to Unity Catalog, and batch-scores the recent joiners (null label).
# MAGIC Expected test AUC ~0.80–0.86. Requires `gold_customer_360`.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog (ask your facilitator)")
dbutils.widgets.text("schema", "", "2. Schema (blank = your own)")

if not dbutils.widgets.get("catalog").strip():
    print("WAITING FOR INPUT — type the catalog into box 1, leave box 2 blank, Run all.")
    dbutils.notebook.exit("NEEDS_CATALOG")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

import mlflow
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.sklearn.autolog(log_models=False)

NUMERIC = ["tenure_years", "total_balance", "avg_account_balance", "num_accounts",
           "txn_count", "debit_swipe_count", "total_spend", "age"]
CATEGORICAL = ["segment", "acquisition_channel", "region", "home_state"]
LABEL = "attrition_flag"
# Never features (target leakage): customer_status, is_retained, attrition_flag.

pdf = spark.table(f"{FQ}.gold_customer_360").select(
    *NUMERIC, *CATEGORICAL, LABEL).toPandas()

pdf[NUMERIC] = pdf[NUMERIC].replace([np.inf, -np.inf], np.nan)
for c in CATEGORICAL:
    pdf[c] = pdf[c].fillna("unknown").astype(str)

train = pdf[pdf[LABEL].notna()].copy()
score = pdf[pdf[LABEL].isna()].copy()
train[LABEL] = train[LABEL].astype(int)
assert train[LABEL].nunique() == 2, "label has only one class"

print(f"train rows: {len(train):,}   score rows: {len(score):,}   "
      f"base rate: {train[LABEL].mean():.3f}")

# COMMAND ----------

X = train[NUMERIC + CATEGORICAL]
y = train[LABEL]
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, stratify=y, random_state=42)

pre = ColumnTransformer([
    ("num", SimpleImputer(strategy="median"), NUMERIC),
    ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")),
                      ("onehot", OneHotEncoder(handle_unknown="ignore"))]), CATEGORICAL),
])
pipe = Pipeline([("pre", pre), ("clf", GradientBoostingClassifier(random_state=42))])

with mlflow.start_run(run_name="churn_gbt"):
    pipe.fit(X_tr, y_tr)
    auc = roc_auc_score(y_te, pipe.predict_proba(X_te)[:, 1])
    mlflow.log_metric("test_auc", auc)
    sig = mlflow.models.infer_signature(X_tr, pipe.predict(X_tr))
    mlflow.sklearn.log_model(pipe, name="model", signature=sig,
                             input_example=X_tr.head(3),
                             registered_model_name=MODEL_NAME)

print(f"test AUC: {auc:.4f}")
if auc > 0.95:
    print("  ⚠️  >0.95 — probable target leakage. Check features.")
elif auc < 0.75:
    print("  ⚠️  <0.75 — weak features.")

# COMMAND ----------

ohe = pipe.named_steps["pre"].named_transformers_["cat"].named_steps["onehot"]
names = NUMERIC + list(ohe.get_feature_names_out(CATEGORICAL))
imp = sorted(zip(names, pipe.named_steps["clf"].feature_importances_), key=lambda t: -t[1])
print("top 10 features:")
for n, v in imp[:10]:
    print(f"  {n:32s} {v:.4f}")

# COMMAND ----------

if len(score):
    score = score.copy()
    score["churn_score"] = pipe.predict_proba(score[NUMERIC + CATEGORICAL])[:, 1]
    score["risk_band"] = np.where(score["churn_score"] >= 0.6, "high",
                          np.where(score["churn_score"] >= 0.35, "medium", "low"))
    (spark.createDataFrame(score).write.mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(f"{FQ}.gold_customers_scored"))
    print(f"scored {len(score):,} recent joiners -> {FQ}.gold_customers_scored")
    display(spark.sql(f"""
        SELECT risk_band, COUNT(*) AS n, ROUND(AVG(churn_score),3) AS avg_score
        FROM {FQ}.gold_customers_scored GROUP BY risk_band ORDER BY avg_score DESC
    """))

# COMMAND ----------

print("✅ Churn model trained, registered, and scored.")
dbutils.notebook.exit("SUCCESS")
