---
name: churn-ml-pipeline
description: MLflow + Unity Catalog patterns for training and batch-scoring a scikit-learn churn/attrition model in a Databricks notebook. Covers experiment paths, NaN/Inf handling, label typing, target-leakage checks, UC three-level naming, and signature inference. Use for the optional churn free-play leg.
metadata:
  adapted_from: databricks-solutions/vibe-coding-workshop-template
  adapted_for: notebook-only churn model, no Feature Store, no serving
  domain: ml
---

# Churn model — notebook path

Scope: one notebook that trains a scikit-learn model on `gold_customer_360`,
logs to MLflow, registers to Unity Catalog, and writes batch predictions back to
Delta. **No Feature Store, no serving endpoint, no Asset Bundle.**

## Critical rules

| # | Rule | Why |
|---|---|---|
| 1 | **Experiment path** must be top-level `/Shared/<name>`. | A `/Users/...` path whose parent folder does not exist fails *silently*. |
| 2 | **Set the registry URI** to `databricks-uc` before registering. | Otherwise the model lands in the legacy workspace registry. |
| 3 | **UC model names need all three levels**: `catalog.schema.model`. | A bare name is rejected. |
| 4 | **Clean NaN *and* Inf before fitting.** `SimpleImputer` only treats NaN as missing. | GradientBoosting raises on non-finite input. |
| 5 | **Cast the label to `int`.** | An object/nullable label gives confusing errors. |
| 6 | **Exclude null-label rows from training** — they are the scoring set. | Recent joiners have `attrition_flag IS NULL`. |
| 7 | **One-hot encode categoricals** with `handle_unknown='ignore'`; never label-encode for a tree. | Label encoding invents an ordering that does not exist. |
| 8 | **Put preprocessing inside a `Pipeline`.** | Prevents train/serving skew. |
| 9 | **Audit for target leakage.** `customer_status` and `is_retained` are near-copies of `attrition_flag` — DROP them from features. | AUC > 0.95 on this problem means leakage, not success. |
| 10 | **Log an inferred signature + input example.** | UC surfaces the schema; consumption is far easier. |

## Setup

```python
import mlflow
mlflow.set_registry_uri("databricks-uc")           # rule 2
mlflow.set_experiment("/Shared/sc_<handle>_attrition")  # rule 1
mlflow.sklearn.autolog(log_models=False)
```

## Features (from gold_customer_360)

```python
NUMERIC = ["tenure_years","total_balance","avg_account_balance","num_accounts",
           "txn_count","debit_swipe_count","total_spend","age"]
CATEGORICAL = ["segment","acquisition_channel","region","home_state"]
LABEL = "attrition_flag"
# rule 9: never include customer_status, is_retained, or attrition_flag as features.
```

## Clean, split, pipeline

```python
import numpy as np
df[NUMERIC] = df[NUMERIC].replace([np.inf, -np.inf], np.nan)   # rule 4
for c in CATEGORICAL: df[c] = df[c].fillna("unknown").astype(str)

train_df = df[df[LABEL].notna()].copy()   # rule 6
score_df = df[df[LABEL].isna()].copy()
train_df[LABEL] = train_df[LABEL].astype(int)   # rule 5
assert train_df[LABEL].nunique() == 2

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import GradientBoostingClassifier

pre = ColumnTransformer([
    ("num", SimpleImputer(strategy="median"), NUMERIC),
    ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")),
                      ("onehot", OneHotEncoder(handle_unknown="ignore"))]), CATEGORICAL),
])
pipe = Pipeline([("pre", pre), ("clf", GradientBoostingClassifier(random_state=42))])
```

## Register + score

```python
with mlflow.start_run(run_name="churn_gbt"):
    pipe.fit(X_train, y_train)
    mlflow.log_metric("test_auc", auc)
    sig = mlflow.models.infer_signature(X_train, pipe.predict(X_train))
    mlflow.sklearn.log_model(pipe, artifact_path="model", signature=sig,
        input_example=X_train.head(3),
        registered_model_name="catalog.schema.attrition_clf")   # rule 3

score_df["churn_score"] = pipe.predict_proba(score_df[NUMERIC+CATEGORICAL])[:, 1]
```

## Sanity checks to report

- training / scoring row counts, label base rate (~0.15–0.20).
- **test AUC — flag < 0.70 (weak features) or > 0.95 (leakage, rule 9).**
  Expected here: ~0.75–0.82.
- top 10 features by importance — expect tenure, balance, activity, age near the top.
