# Workshop prompts

Every prompt in the session, in order. Copy them into **Genie Code in agent
mode**, one leg at a time.

Two rules that make this a workshop rather than a copy-paste demo:

1. **Read what Genie Code builds before you run it.** Each leg ends with a
   question about what it actually wrote. That is where the learning is.
2. **Check your numbers against the expected values.** If yours differ by more
   than a few percent, something is off — flag it before moving on.

> Your dataset is seeded from your username, so your numbers will be *close to*
> but not identical to the expected values below, and not identical to your
> neighbour's. Ranges are given where it matters.

## First — write your schema name down

`setup/00_setup` printed a `schema:` line near the top of its output:

```
schema      : km_catalog.sc_firstname_lastname
```

**Copy that whole value onto a sticky note.** Every prompt below has a
`<SCHEMA>` placeholder; paste the real value in each time. Paste the literal
name rather than typing `<SCHEMA>` — Genie Code otherwise guesses, and usually
guesses wrong. Your Volume of raw files is at `<SCHEMA>`'s `landing` volume:
`/Volumes/<catalog>/<your_schema>/landing`.

---

## Leg 0 — Look at the raw data (medallion intro, ~6 min)

Before you build anything, see what you are working with. This is the "raw
landing zone" of a **medallion architecture** — deliberately messy, exactly as
data arrives from source systems.

### Prompt

```
In the Volume /Volumes/<catalog>/<your_schema>/landing there are raw banking
files, one folder per source: customers/, accounts/, branches/,
customer_events/ (CSV) and card_transactions/ (JSON shards).

Profile them for me without creating any tables. For each file: row count,
columns, and a sample. Then specifically find and count the data-quality
problems: duplicate customer_id and txn_id values, home_state values that are
full names instead of 2-letter codes, date_of_birth values in a non-ISO format,
missing date_of_birth, negative or absurdly large account balances, and
account rows whose customer_id matches no customer.

Just report — do not fix anything yet. Use serverless.
```

### Expected

Roughly: **6,060** customer rows but **6,000** distinct `customer_id`
(≈60 duplicates), **~11,000** accounts, **~63,000** distinct transactions,
**~6,900** events, **40** branches. Planted problems: ~300 long-form states,
~316 alt-format dates of birth, ~25 missing DOB, ~33 negative balances, ~12
hundred-times balance typos, ~25 orphan accounts, ~25 hundred-times transaction
amounts, ~90 duplicate transaction rows.

### Talk about it

Which layer of the medallion does each fix belong in? (Cleaning/typing →
Silver; business rollups → Gold.) Why keep Bronze dirty at all? Answer: Bronze
is your audit trail — if a cleaning rule is wrong, you can always rebuild from
the untouched source.

---

## Leg 1 — Build the medallion with DLT (20 min)

Now build Bronze → Silver → Gold as a **Lakeflow Declarative Pipeline** (DLT),
not as plain notebooks. Load the pipeline skill first:

```
Read the skill at
/Workspace/Users/<your-email>/.assistant/skills/dlt-lakeflow-pipeline/SKILL.md
and follow its rules for everything below.
```

### Prompt

```
Build a Lakeflow Declarative Pipeline (DLT) in SQL that ingests the raw files
in /Volumes/<catalog>/<your_schema>/landing and builds a medallion. Write it as
a pipeline source file, then create and run the pipeline targeting <SCHEMA>.

BRONZE — streaming tables, Auto Loader (STREAM read_files), raw and as-landed,
one per source file: bronze_customers, bronze_accounts, bronze_branches,
bronze_customer_events, bronze_card_transactions.

SILVER — materialized views with EXPECTATIONS:
- silver_customers: one row per customer_id (keep the latest updated_at).
  Parse date_of_birth from BOTH 'yyyy-MM-dd' and 'MM/dd/yyyy'. Standardize
  home_state to a 2-letter code (the data mixes 'TN' with 'Tennessee' — handle
  all 12 states). Derive age, age_band ('18-25','26-35','36-45','46-55',
  '56-65','66-75','76+'), tenure_years, moved_from_bank (acquisition_channel =
  'switch_kit'), and is_retained (customer_status <> 'attrited').
  EXPECT and DROP rows with a missing/unparseable DOB or an age outside 18-110.
- silver_accounts: typed; EXPECT and DROP negative balances, balances over
  10,000,000, and accounts whose customer_id is not in silver_customers.
- silver_card_transactions: one row per txn_id; EXPECT and DROP amounts over
  100,000.
- silver_customer_events and silver_branches: typed.
- Also build silver_customers_quarantine and silver_accounts_quarantine that
  KEEP the dropped rows with a quarantine_reason column.

GOLD — materialized views:
- gold_customer_360: one row per customer with age, age_band, region (from
  branch), segment, acquisition_channel, source_bank, moved_from_bank,
  tenure_years, customer_status, is_retained, attrition_flag, num_accounts,
  total_balance, avg_account_balance, txn_count, debit_swipe_count (debit AND
  channel='swipe'), debit_txn_count, total_spend.
- gold_transaction_facts: one row per transaction joined to the customer's age,
  age_band, segment, home_state and region.
- gold_balance_by_age_band: age_band -> customer_count, avg_balance, total_balance.
- gold_retention_summary: for moved_from_bank customers, by source_bank ->
  moved_in, stayed, left_us, retention_rate.

Then run the pipeline and show me the pipeline's data-quality metrics (how many
rows each expectation dropped) and the row count of every table.
```

### Expected

| Table | Rows |
|---|---|
| `silver_customers` | 5,940–5,985 |
| `silver_customers_quarantine` | 15–40 |
| `silver_accounts` | 10,850–11,020 |
| `silver_accounts_quarantine` | 40–85 |
| `silver_card_transactions` | 63,300–64,600 |
| `gold_customer_360` | matches `silver_customers` |
| `gold_balance_by_age_band` | 7 rows |
| `gold_retention_summary` | up to 9 rows (8 source banks + unknown) |

The 60 duplicate customer rows collapse to one each; ~25 rows drop for a bad
DOB; accounts drop ~33 negative + ~12 outlier + ~25 orphan.

### Read the code

- Find the expectations. What is the difference between `ON VIOLATION DROP ROW`
  and just letting a bad row through? Where would you use `FAIL UPDATE` instead?
- Look at the pipeline graph in the UI. It figured out that gold depends on
  silver depends on bronze without you saying so — how? (It reads the table
  references in each query.)
- Bronze is a **streaming table**; silver and gold are **materialized views**.
  Why that split? (Auto Loader ingests new files incrementally into bronze;
  silver/gold recompute the business logic from the full history.)

---

## Leg 2 — Semantic layer (16 min)

One governed definition of every headline metric, so "average balance" and
"retention rate" mean the same thing in the dashboard, in Genie, and in SQL.
Load the metric-view skill first:

```
Read the skill at
/Workspace/Users/<your-email>/.assistant/skills/metric-views/SKILL.md
and follow its YAML and query rules.
```

### Prompt

```
Create a Unity Catalog metric view <SCHEMA>.mv_banking_metrics over
<SCHEMA>.gold_customer_360, using CREATE OR REPLACE VIEW ... WITH METRICS
LANGUAGE YAML. Version 1.1.

Dimensions: Age (age), Age Band (age_band), Gender, State (home_state), Region,
Segment, Acquisition Channel, Source Bank, Moved From Bank (moved_from_bank),
Customer Status, Retained (is_retained).

Measures:
- Customer Count = COUNT(1)
- Average Balance = AVG(total_balance)
- Total Balance = SUM(total_balance)
- Median Balance = PERCENTILE(total_balance, 0.5)
- Debit Card Swipes = SUM(debit_swipe_count)
- Total Transactions = SUM(txn_count)
- Retention Rate = AVG(CASE WHEN is_retained THEN 1.0 ELSE 0.0 END)
- Attrition Rate = AVG(CASE WHEN customer_status='attrited' THEN 1.0 ELSE 0.0 END)

Then govern it: add a COMMENT to the metric view describing it, and apply Unity
Catalog tags so it is discoverable — tag the view with domain='retail_banking'
and certified='true'. Finally, run three validation queries with MEASURE():
1. Average Balance where Age BETWEEN 61 AND 65
2. Debit Card Swipes where Age BETWEEN 21 AND 25
3. Customer Count and Retention Rate where Moved From Bank = true
```

### Expected

The three validation queries are the workshop's anchor questions:

1. **Average balance, ages 61–65:** roughly **$23,000–$35,000** (n ≈ 470–500).
2. **Debit card swipes, ages 21–25:** roughly **2,300–2,750**.
3. **Moved to our bank:** roughly **1,020–1,170** customers; **retention rate
   76–80%**.

### Read the code

- Every measure must be queried with `MEASURE(\`Average Balance\`)`, and
  `SELECT *` is not allowed. Why? (A metric view defers aggregation to query
  time, so it can safely re-aggregate ratios like retention rate at any grain.)
- `Average Balance` is `AVG(total_balance)`. If you group by `Region`, is the
  region average a simple average of the age-band averages? (No — the metric
  view re-aggregates from the source rows, which is the whole point.)

---

## Leg 3 — Dashboard (14 min)

Load the dashboard skill first:

```
Read the skill at
/Workspace/Users/<your-email>/.assistant/skills/databricks-aibi-dashboards/SKILL.md
and follow its widget and grid rules.
```

### Prompt

```
Create a Databricks AI/BI dashboard called "Strategy Corps — Retail Banking
Overview" as a .lvdash.json file, then deploy it to my workspace.

Datasets from <SCHEMA>: gold_customer_360, gold_balance_by_age_band,
gold_retention_summary, gold_transaction_facts.

Widgets:
1. Four KPI counters across the top — total customers, total balance,
   switch-in retention rate (from gold_retention_summary: sum(stayed)/sum(moved_in)),
   and total debit card swipes.
2. A bar chart of average balance by age_band from gold_balance_by_age_band.
3. A bar chart of debit card swipe counts by age_band — aggregate
   gold_transaction_facts where card_type='debit' and channel='swipe'.
4. A bar chart from gold_retention_summary: stayed vs left_us by source_bank.
5. A choropleth map of the US shaded by customer count per home_state from
   gold_customer_360.
6. A table of the source-bank retention summary with moved_in, stayed,
   retention_rate.

Format money as currency with no decimals, rates as a percentage with one
decimal. Use the 6-column grid. Deploy it and give me the URL.
```

### Read the code

Open the `.lvdash.json` it generated. Find one KPI counter's spec — query,
encoding, formatting, position are all declarative text. That is why an agent
can write a dashboard, and why you can put one in version control.

---

## Leg 4 — Genie agent (16 min)

Build the chatbot a Strategy Corps bank customer would actually query. This is
the payoff: a natural-language agent over your gold layer and metric view.

### Prompt

```
Create a Genie space called "Retail Banking Assistant" over these tables in
<SCHEMA>: gold_customer_360, gold_transaction_facts, and the metric view
mv_banking_metrics.

Add general instructions: this is a retail bank's customer analytics assistant.
"Balance" means total_balance. "Debit card swipes" means transactions where
card_type='debit' and channel='swipe'. "Moved to our bank" / "switched to us"
means moved_from_bank = true (acquisition_channel='switch_kit'). "Stayed" /
"retained" means is_retained = true. Age filters are on the age column.

Add these as example / sample questions with trusted SQL where helpful:
- "What is the average balance for people between ages 61 and 65?"
- "How many debit card swipes for customers aged 21 to 25?"
- "How many people moved to our bank, and how many of them stayed?"
- "What is the retention rate of switched-in customers by source bank?"

Then actually ask the agent all four questions and show me the SQL it generated
and the answers.
```

### Expected answers

These are the three questions Strategy Corps' bank customers care about — verify
the agent gets them right:

1. **Average balance, ages 61–65:** ~$23,000–$35,000.
2. **Debit card swipes, ages 21–25:** ~2,300–2,750.
3. **Moved to our bank:** ~1,020–1,170; **stayed:** ~800–907 (retention ~76–80%).

### Read the code

- Open the SQL Genie generated for "average balance, ages 61–65." Did it filter
  `age BETWEEN 61 AND 65`, or did it use the wrong column? This is why the
  instructions and the metric view matter — they pin the vocabulary.
- Ask a deliberately ambiguous follow-up: *"what about younger people?"* Watch
  how it interprets "younger." What instruction would remove the ambiguity?

---

## When Genie Code gets it wrong

It will, sometimes. Correction prompts that work:

**Pipeline expectation dropped too many / too few rows**
```
silver_customers has <N> rows but I expected about 5,975. Show me each
expectation and the dedup logic, and confirm you keep exactly one row per
customer_id (the latest updated_at) and only drop rows with a bad DOB or an
age outside 18-110.
```

**DOB parsed to null**
```
age is null for many customers. date_of_birth arrives in TWO formats:
'yyyy-MM-dd' and 'MM/dd/yyyy'. Use COALESCE(TRY_TO_DATE(x),
TRY_TO_DATE(x,'MM/dd/yyyy')) and recompute.
```

**Metric view won't query**
```
That failed. Metric view measures must be wrapped in MEASURE(), SELECT * is
not supported, and names with spaces need backticks. Rewrite the query that way.
```

**Genie answered with the wrong column**
```
That used the wrong field. "Balance" is total_balance and age filters are on
the age column in gold_customer_360. Update the instruction and re-answer.
```

**It tried to hand-write silver/gold as notebooks instead of a pipeline**
```
Stop — this leg is a Lakeflow Declarative Pipeline (DLT). Put bronze/silver/gold
in a pipeline source file with STREAM read_files and EXPECT constraints, and
create the pipeline. Not plain notebooks.
```

**It wandered off scope**
```
That is more than I asked for. Requirement was exactly: <RESTATE>. Redo just
that, nothing else.
```

---

## Free play (optional, if time)

See `FREE_PLAY_MENU.md`, or ask your facilitator for the handout. The headline
option is an optional churn model: predict which customers will attrite, from
the same gold layer.
