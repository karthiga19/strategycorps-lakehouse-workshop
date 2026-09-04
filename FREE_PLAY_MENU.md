# Free play

Pick **one**. Work with your partner. We demo at the end and the room votes on
the best result. There is no requirement to finish — a half-built idea you can
explain is worth more than a finished one you cannot.

---

## 1 · Predict who will churn 🔥

**The headline option.** You have `attrition_flag` on customers who have been
with the bank long enough to have an outcome; recent joiners have `NULL` — they
are your scoring set. Load the churn skill first:

```
Read the skill at
/Workspace/Users/<your-email>/.assistant/skills/churn-ml-pipeline/SKILL.md
and follow its rules.
```

```
Using <SCHEMA>.gold_customer_360, train a model that predicts attrition_flag.

Training set: customers where attrition_flag IS NOT NULL. Recent joiners
(attrition_flag IS NULL) are the scoring set.

Features: numeric — tenure_years, total_balance, avg_account_balance,
num_accounts, txn_count, debit_swipe_count, total_spend, age; categorical —
segment, acquisition_channel, region, home_state.

Use a scikit-learn Pipeline (median impute + passthrough numeric; most-frequent
impute + one-hot for categoricals), GradientBoostingClassifier, stratified
75/25 split, random_state=42. Set the MLflow experiment to
/Shared/sc_<your-handle>_attrition, autolog, and register to Unity Catalog as
<SCHEMA>.attrition_clf. Print the test AUC and the top 10 features.

Then batch-score the NULL-flag customers into <SCHEMA>.gold_customers_scored
with a churn_score and a risk_band ('high' >= 0.6, 'medium' >= 0.35, else 'low').
```

**Expected: test AUC ~0.80–0.86.** Above 0.95 you have leaked the target —
`customer_status` or `is_retained` is a near-copy of `attrition_flag`; remove
it. Top features should be roughly tenure, balance, activity, and age.

**Then:** add a "retention watchlist" page to your dashboard showing high-risk
customers by segment and region.

---

## 2 · Read the churn reasons with AI

**No ML needed.** Every `attrited` event has a free-text-ish `reason`.

```
Using ai_classify on the reason column in <SCHEMA>.silver_customer_events where
event_type = 'attrited', bucket each into: pricing, service, life_event,
competitor. Then show me the count by bucket and 5 example rows per bucket.
```

---

## 3 · Break the pipeline on purpose

**Best for understanding what data quality actually does.**

```
Add 20 deliberately bad rows to the landing files (or a bronze table): some
with negative balances, some with an unparseable date_of_birth, some accounts
with a customer_id that doesn't exist. Re-run the pipeline and show me the
before/after data-quality metrics — did every bad row get caught by an
expectation, and land in the right quarantine table? If any got through, which
expectation is missing?
```

---

## 4 · Segment deep-dive with the metric view

```
Using <SCHEMA>.mv_banking_metrics, build me a table that compares mass_market,
affluent and private segments on: customer count, average balance, debit card
swipes per customer, and retention rate. Then tell me in plain language which
segment is most valuable and which is most at risk, and what you'd do about it.
```

---

## 5 · Explain it to a bank exec

**No code. Genuinely useful.**

```
Read through every table in <SCHEMA> and the pipeline and metric view I built
today, then write a one-page summary for a non-technical bank executive: what
raw data we started with, what the pipeline does at each medallion layer, what
the semantic layer gives us, and what business questions the Genie agent can now
answer. No jargon; explain "medallion" if you use it.
```

---

## Demo time

Two minutes each: What did you try? Show the thing. What surprised you about
working this way? That third question is the one we actually care about.
