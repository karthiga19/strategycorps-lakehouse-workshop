"""Regenerate the answer key in PROMPTS.md.

Builds the dataset (post-defects) across several seeds and prints the row
counts, the planted-defect breakdown, and the answers to the three anchor
questions -- so the facilitator can state expected ranges out loud and spot a
participant whose numbers are off.

    python3 tools/verify_expected_counts.py
"""

import os
import statistics
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "setup"))

import _generator as gen  # noqa: E402

TODAY = gen.TODAY
SEEDS = [12345, 1, 2, 42, 777, 2024, 99999]


def age_on(dob_iso):
    d = date.fromisoformat(dob_iso)
    return (TODAY - d).days // 365


def summarize(seed):
    branches, customers, accounts, transactions, events = gen.build(seed)
    # Keep a clean copy of pre-defect customers for the "truth" answers, then
    # inject defects to measure raw/dedup counts.
    truth = [c for c in customers]
    defects = gen.inject_defects(seed, customers, accounts, transactions)

    distinct_cust = len({c["customer_id"] for c in customers})
    distinct_txn = len({t["txn_id"] for t in transactions})

    # Anchor Q1: average total balance for ages 61-65 (from clean customers).
    bal_61_65 = [c["_total_balance"] for c in truth if 61 <= c["_age"] <= 65]
    avg_bal_61_65 = statistics.mean(bal_61_65) if bal_61_65 else 0.0

    # Anchor Q2: debit card swipes for customers aged 21-25.
    acct_owner = {}
    for c in truth:
        pass
    # map account_id -> customer age via accounts + customers (pre-orphan copy)
    cust_age = {c["customer_id"]: c["_age"] for c in truth}
    # rebuild account->customer from a fresh build to avoid defect-mangled ids
    b2, c2, a2, t2, e2 = gen.build(seed)
    acct_cust = {a["account_id"]: a["customer_id"] for a in a2}
    age2 = {c["customer_id"]: c["_age"] for c in c2}
    debit_swipes_21_25 = 0
    for t in t2:
        cid = acct_cust.get(t["account_id"])
        if cid is None:
            continue
        ag = age2.get(cid)
        if ag is not None and 21 <= ag <= 25 and t["card_type"] == "debit" \
                and t["channel"] == "swipe":
            debit_swipes_21_25 += 1

    # Anchor Q3: moved to our bank (switch_kit) and who stayed (not attrited).
    moved = [c for c in truth if c["acquisition_channel"] == "switch_kit"]
    stayed = [c for c in moved if c["customer_status"] != "attrited"]
    retention = (len(stayed) / len(moved)) if moved else 0.0

    return {
        "customers_raw": len(customers),
        "customers_distinct": distinct_cust,
        "accounts": len(accounts),
        "txn_raw": len(transactions),
        "txn_distinct": distinct_txn,
        "events": len(events),
        "branches": len(branches),
        "avg_bal_61_65": avg_bal_61_65,
        "n_61_65": len(bal_61_65),
        "debit_swipes_21_25": debit_swipes_21_25,
        "moved": len(moved),
        "stayed": len(stayed),
        "retention": retention,
        "attr_base": sum(1 for c in truth if c["attrition_flag"] == 1)
        / max(1, sum(1 for c in truth if c["attrition_flag"] is not None)),
        "labelled": sum(1 for c in truth if c["attrition_flag"] is not None),
        "defects": defects,
    }


def rng_range(vals):
    return f"{min(vals):,.0f}-{max(vals):,.0f}"


def main():
    rows = [summarize(s) for s in SEEDS]

    def col(k):
        return [r[k] for r in rows]

    print("=" * 68)
    print("  ROW COUNTS across seeds", SEEDS)
    print("=" * 68)
    print(f"  customers (raw, with dupes) : {rng_range(col('customers_raw'))}")
    print(f"  customers (distinct)        : {rng_range(col('customers_distinct'))}")
    print(f"  accounts                    : {rng_range(col('accounts'))}")
    print(f"  card_transactions (raw)     : {rng_range(col('txn_raw'))}")
    print(f"  card_transactions (distinct): {rng_range(col('txn_distinct'))}")
    print(f"  customer_events             : {rng_range(col('events'))}")
    print(f"  branches                    : {rows[0]['branches']}")
    print()
    print("  ANCHOR QUESTION ANSWERS")
    print(f"  Q1 avg balance ages 61-65   : ${min(col('avg_bal_61_65')):,.0f}-"
          f"${max(col('avg_bal_61_65')):,.0f}  (n={rng_range(col('n_61_65'))})")
    print(f"  Q2 debit swipes ages 21-25  : {rng_range(col('debit_swipes_21_25'))}")
    print(f"  Q3 moved to our bank        : {rng_range(col('moved'))}")
    print(f"     of those, stayed         : {rng_range(col('stayed'))}")
    print(f"     retention rate           : {min(col('retention')):.1%}-"
          f"{max(col('retention')):.1%}")
    print()
    print("  CHURN LABEL (optional ML leg)")
    print(f"  labelled customers          : {rng_range(col('labelled'))}")
    print(f"  attrition base rate         : {min(col('attr_base')):.1%}-"
          f"{max(col('attr_base')):.1%}")
    print()
    print("  PLANTED DEFECTS (seed 12345)")
    for k, v in rows[0]["defects"].items():
        print(f"    {k:28s} {v}")


if __name__ == "__main__":
    main()
