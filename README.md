# Strategy Corps Lakehouse Workshop

Build a complete banking data product in 90 minutes — from raw files to a
natural-language agent — by **describing what you want to Genie Code** rather
than writing the code yourself.

The data is synthetic retail banking: customers, accounts, card transactions,
lifecycle events (who joined, who switched banks, who left), and branches. By
the end you have a governed lakehouse and a chatbot that answers the questions a
bank actually asks:

- *"What is the average balance for people between ages 61 and 65?"*
- *"How many debit card swipes for customers aged 21–25?"*
- *"How many people moved to our bank, and who stayed?"*

---

## Start here

👉 **[PREREQS.md](PREREQS.md)** — do this before the session. About 10 minutes.

👉 **[PROMPTS.md](PROMPTS.md)** — every prompt for the session, in order.

---

## What you will build

```
   Volume (raw CSV + JSON) — the landing zone
          │
          ▼
   ┌──────────────┐
   │   MEDALLION   │   Leg 1 — a Lakeflow Declarative Pipeline (DLT)
   │  via DLT      │
   │               │   BRONZE  Auto Loader, raw, as-landed  (5 tables)
   │               │   SILVER  typed, deduped, EXPECTATIONS (quarantine)
   │               │   GOLD    customer-360 + facts + rollups
   └──────┬───────┘
          ▼
   ┌──────────────┐   metric view: one governed definition of every KPI
   │  SEMANTIC     │   Leg 2 — mv_banking_metrics + UC tags/domain
   └──────┬───────┘
          │
          ├──────────────► Dashboard   Leg 3 — AI/BI KPIs, charts, map
          │
          └──────────────► Genie agent Leg 4 — the natural-language chatbot
```

By the end you have a running pipeline, ~11 tables, a governed metric view, a
deployed dashboard, and a Genie agent — none of which you wrote by hand.

---

## The 90 minutes

| Time | Leg | What |
|---|---|---|
| 0–08 | Setup check | Verify everyone's raw files landed |
| 08–14 | **Leg 0 — Explore** | Medallion intro; profile the raw, dirty data |
| 14–34 | **Leg 1 — DLT** | Build bronze→silver→gold as a declarative pipeline |
| 34–38 | Catch-up | Anyone behind runs a checkpoint |
| 38–54 | **Leg 2 — Semantic** | Metric view + governance |
| 54–68 | **Leg 3 — Dashboard** | AI/BI dashboard on the gold layer |
| 68–84 | **Leg 4 — Genie** | Natural-language agent; verify the anchor questions |
| 84–90 | Debrief / free play | Optional churn model; production considerations |

---

## Repo layout

```
PREREQS.md                    pre-session setup
PROMPTS.md                    all workshop prompts + expected numbers

setup/
  00_setup.py                 ← the only setup notebook you open
  _generator.py               stdlib-only data generator (no pip install)

config.py                     shared plumbing: widgets + all the naming.
                              Pulled in via %run — you never open it.

pipeline/
  medallion_dlt.sql           reference DLT pipeline (the Leg 1 answer key)

semantic/
  mv_banking_metrics.yaml     reference metric view (the Leg 2 answer key)

checkpoints/
  00_fast_catchup.py          copy finished tables from the shared schema
  01_medallion.py             catch-up: build silver+gold with plain SQL
  02_semantic.py              catch-up: create the metric view
  03_churn_ml.py              optional: trained churn model + scored customers

tools/                        dev utilities, not used in the session
  validate_signal.py          checks the churn signal lands in the AUC band
  verify_expected_counts.py   regenerates the answer key in PROMPTS.md

.assistant/skills/            Genie Code skills — 00_setup installs these
```

## Catalog and schema

`setup/00_setup` is the single notebook to run before the session. **Every
notebook needs two passes the first time**: Run all, it stops with `WAITING FOR
INPUT` and shows two input boxes, you fill in the catalog, Run all again. That
is by design — the boxes do not exist until the first cell runs.

| Box | What to enter |
|---|---|
| **catalog** | The workshop catalog — your facilitator provides it |
| **schema** | Leave blank to get your own private schema, `sc_<your-name>` |

Everyone works in their own schema inside the shared catalog, so nobody
overwrites anybody else's tables.

---

## Falling behind is fine

- **`checkpoints/00_fast_catchup`** — copies finished tables into your schema in
  seconds, so you can start any leg even if you missed the one before.
- **`checkpoints/01_medallion` / `02_semantic`** — build the correct output from
  scratch. These are also the answer key for their legs, so they are worth
  reading either way.

Nobody should sit blocked. If you are stuck, put up a red sticky note.

---

## Notes on the data

The raw files are deliberately dirty. Planted defects include duplicate
`customer_id`s and `txn_id`s, mixed state formats (`"TN"` and `"Tennessee"`),
two different date-of-birth formats, missing DOBs, negative and 100×-inflated
balances, and orphan accounts. Catching these with pipeline **expectations** is
the DLT leg.

Your dataset is seeded from your username, so your row counts differ slightly
from your neighbour's. `PROMPTS.md` gives expected ranges.

---

## Attribution

Structure adapted from the Databricks
[vibe-coding-workshop-template](https://github.com/databricks-solutions/vibe-coding-workshop-template)
and a sibling JMI lakehouse workshop, re-themed for retail banking and
re-sequenced to the DLT → semantic-layer → dashboard → Genie path.
