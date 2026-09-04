---
name: dlt-lakeflow-pipeline
description: Building a Lakeflow Declarative Pipeline (DLT) in SQL for a medallion — Auto Loader streaming tables for bronze, materialized views with EXPECTATIONS for silver/gold, dedup, quarantine, and creating/running the pipeline. Use when building bronze→silver→gold as a declarative pipeline rather than notebooks.
metadata:
  adapted_for: notebook-driven workshop, serverless pipeline, one source file per pipeline
  domain: pipelines
---

# Lakeflow Declarative Pipelines (DLT) — SQL medallion

A pipeline is a **source file of SQL declarations** plus a **pipeline object**
that runs them. You declare each table as a query; the pipeline infers the
bronze→silver→gold dependency graph from the table references and runs them in
order, tracking data-quality metrics for every expectation.

## The two table kinds

| Kind | Use for | Reads |
|---|---|---|
| **Streaming table** | Bronze ingest — process each new file once | `STREAM read_files(...)` (Auto Loader) |
| **Materialized view** | Silver / gold — recompute business logic from full history | plain `SELECT` from other pipeline tables |

Rule of thumb: **bronze = streaming table** (incremental file ingest),
**silver/gold = materialized view** (dedup, joins, aggregates need the whole
dataset).

## Bronze — Auto Loader

```sql
CREATE OR REFRESH STREAMING TABLE bronze_customers
  COMMENT 'Raw customers as landed.'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing}/customers/',
  format => 'csv', header => 'true', inferColumnTypes => 'false'
);
```

- `STREAM read_files(...)` is Auto Loader — it ingests only new files each run.
- **Point it at a directory, not a single file.** Streaming `read_files` monitors
  a *folder* — an exact file path (`.../customers.csv`) fails with "not a
  directory". Land each source in its own subfolder (`landing/customers/`, …).
- `inferColumnTypes => 'false'` keeps CSV as strings, so casting is a visible
  silver step. JSON: `format => 'json'` (types inferred, which is fine for JSON).
- `${landing}` is a **pipeline configuration** value you set when creating the
  pipeline (see below) — do not hardcode the Volume path in the source.
- Reference other pipeline tables by **unqualified name** (`bronze_customers`),
  never `catalog.schema.bronze_customers`. The pipeline's default catalog/schema
  qualifies them.

## Silver — expectations and dedup

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  CONSTRAINT valid_dob     EXPECT (date_of_birth IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT plausible_age EXPECT (age BETWEEN 18 AND 110)     ON VIOLATION DROP ROW
)
  COMMENT 'One row per customer, typed, expectations enforced.'
AS
SELECT *, FLOOR(DATEDIFF(current_date(), date_of_birth)/365.25) AS age
FROM bronze_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
```

**Expectation actions:**

| Clause | Effect |
|---|---|
| *(none)* | Bad rows are **kept**, but counted in the pipeline's DQ metrics (warn) |
| `ON VIOLATION DROP ROW` | Bad rows are dropped from this table |
| `ON VIOLATION FAIL UPDATE` | The whole pipeline update fails — use for invariants that must never break |

**Dedup**: `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY ... DESC) = 1`
in a materialized view. Never `DISTINCT` (it dedups whole rows, not by key) and
never `GROUP BY` (it collapses columns).

**Quarantine (keep the bad rows too)**: an expectation with `DROP ROW` discards
violating rows silently. To *keep* them for inspection, add a second
materialized view selecting the rows with the **inverse** predicate and a
`quarantine_reason` column. That pair — clean table + quarantine table — is the
standard pattern.

## Two date formats, state standardization

```sql
COALESCE(TRY_TO_DATE(date_of_birth), TRY_TO_DATE(date_of_birth, 'MM/dd/yyyy')) AS dob
```

```sql
CASE UPPER(home_state)
  WHEN 'TENNESSEE' THEN 'TN' WHEN 'TEXAS' THEN 'TX' /* ...all states... */
  ELSE UPPER(home_state) END AS home_state
```

## Gold — materialized views

Plain aggregate/join materialized views over silver. Use `COUNT_IF(cond)` for
conditional counts (e.g. `COUNT_IF(card_type='debit' AND channel='swipe')`), and
`LEFT JOIN` + `COALESCE(x, 0)` so customers with no transactions still appear.

## Creating and running the pipeline

Write the SQL to one source file, then create a **serverless** pipeline pointing
at it, with the default catalog + target schema set to the participant's schema
and a `landing` configuration value:

```bash
databricks pipelines create --json '{
  "name": "sc_medallion_<handle>",
  "serverless": true,
  "catalog": "<catalog>",
  "schema": "<your_schema>",
  "configuration": { "landing": "/Volumes/<catalog>/<your_schema>/landing" },
  "libraries": [ { "file": { "path": "/Workspace/Users/<you>/.../pipeline/medallion_dlt.sql" } } ]
}'
databricks pipelines start-update --pipeline-id <id>
```

Or create it in the UI: **Pipelines → Create pipeline → serverless**, add the
source file, set catalog/schema, add the `landing` config key.

## Before declaring it done

1. Bronze tables are `STREAMING TABLE` with `STREAM read_files`; silver/gold are
   `MATERIALIZED VIEW`.
2. No hardcoded Volume path — it comes from `${landing}` config.
3. Table references inside the pipeline are unqualified.
4. Each silver table has expectations; the clean/quarantine pair covers every
   planted defect.
5. Run the pipeline and read the DQ metrics in the UI — confirm the dropped-row
   counts match the expected defect counts.
