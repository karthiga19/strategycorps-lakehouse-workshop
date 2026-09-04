"""Synthetic retail-banking data for the Strategy Corps lakehouse workshop.

Standard library only, on purpose: a workshop should not depend on a
`%pip install` succeeding on someone's compute, and bank workspaces often
restrict outbound network access. So no Faker, no external packages.

The module is deliberately free of any Spark or Databricks reference so it can
be imported and exercised on a laptop -- see `tools/validate_signal.py`, which
uses it to check that the planted attrition signal still lands in the target
AUC band for the optional churn-model leg.

The data is shaped to answer three anchor questions a bank analyst asks the
Genie agent at the end of the workshop:

    1. "What is the average balance for people between ages 61 and 65?"
    2. "How many debit card swipes for customers aged 21-25?"
    3. "How many people moved to our bank, and who stayed?"

Public entry point:

    generate(seed, outdir) -> dict of row counts
"""

import csv
import json
import math
import os
import random
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

# Strategy Corps is headquartered near Nashville, TN, so a Tennessee-weighted
# footprint is deliberate -- it makes the "TN" / "Tennessee" standardisation
# defect land on a large slice of the data rather than a rounding error. Each
# state carries a region, which becomes a dimension in the semantic layer.
STATES = [
    ("TN", "Tennessee", "South", 0.16),
    ("TX", "Texas", "South", 0.10),
    ("GA", "Georgia", "South", 0.08),
    ("FL", "Florida", "South", 0.09),
    ("NC", "North Carolina", "South", 0.07),
    ("OH", "Ohio", "Midwest", 0.06),
    ("IL", "Illinois", "Midwest", 0.07),
    ("MI", "Michigan", "Midwest", 0.05),
    ("CA", "California", "West", 0.09),
    ("AZ", "Arizona", "West", 0.05),
    ("NY", "New York", "Northeast", 0.09),
    ("PA", "Pennsylvania", "Northeast", 0.09),
]

# (segment, weight, median total balance, log-spread, min accounts, max accounts)
SEGMENTS = [
    ("mass_market", 0.70, 3500, 0.85, 1, 2),
    ("affluent", 0.24, 22000, 0.75, 2, 3),
    ("private", 0.06, 120000, 0.70, 2, 4),
]

ACCOUNT_TYPES = [
    ("checking", 0.46),
    ("savings", 0.34),
    ("money_market", 0.13),
    ("cd", 0.07),
]

CARD_TYPES = [("debit", 0.65), ("credit", 0.35)]

# Transaction channels. "swipe" is called out because "debit card swipes" is one
# of the anchor questions -- younger customers skew toward swipe/contactless.
DEBIT_CHANNELS = [("swipe", 0.45), ("contactless", 0.25), ("atm", 0.16), ("online", 0.14)]
CREDIT_CHANNELS = [("online", 0.42), ("swipe", 0.30), ("contactless", 0.28)]

MERCHANT_CATEGORIES = [
    ("grocery", 0.20, 64), ("restaurant", 0.18, 38), ("gas", 0.13, 47),
    ("retail", 0.16, 82), ("travel", 0.07, 310), ("utilities", 0.08, 145),
    ("healthcare", 0.06, 190), ("entertainment", 0.07, 55), ("atm_cash", 0.05, 120),
]

# How each customer was acquired. "switch_kit" means they moved to us from
# another bank -- that population, and whether they then stayed, is anchor
# question 3.
ACQUISITION_CHANNELS = [
    ("branch", 0.34), ("online", 0.27), ("referral", 0.14),
    ("switch_kit", 0.18), ("employer", 0.07),
]

# Competitor banks a switched-in customer came from. Populated only for
# switch_kit acquisitions; blank otherwise.
SOURCE_BANKS = [
    "First National", "Metro Savings", "Union Trust", "Pinnacle Federal",
    "Harbor Community Bank", "Summit Credit Union", "Cornerstone Bank",
    "Riverside Financial",
]

BRANCH_NAMES = [
    "Downtown", "Midtown", "Westgate", "Northpark", "Southside", "Riverfront",
    "Oakwood", "Highland", "Lakeside", "Meridian", "Brookfield", "Fairview",
    "Sterling", "Kingsport", "Belmont", "Crossroads", "Parkway", "Heritage",
    "Cedar Grove", "Stonebridge", "Whitfield", "Aurora", "Kessler", "Marchetti",
    "Brightwater", "Halcyon", "Northline", "Everly", "Copperfield", "Sablewood",
    "Lumen", "Ironwood", "Thistle", "Verity", "Ashford", "Larkspur",
    "Continental", "Wrenfield", "Solstice", "Havenbrook",
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Chris", "Karen", "Daniel", "Nancy",
    "Matthew", "Lisa", "Anthony", "Betty", "Mark", "Sandra", "Ava", "Noah",
    "Liam", "Emma", "Olivia", "Sophia", "Mateo", "Aisha", "Wei", "Priya",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Clark", "Lewis", "Nguyen",
    "Patel", "Kim", "Okafor", "Rossi", "Chen", "Khan", "Silva", "Adams",
]

# Age bands used to draw customer ages. Kept broad here; the workshop derives
# the analytic age_band in the pipeline. Weights ensure the 21-25 and 61-65
# slices (the anchor questions) are well populated.
AGE_BANDS = [
    (18, 25, 0.14), (26, 35, 0.19), (36, 45, 0.18), (46, 55, 0.17),
    (56, 65, 0.16), (66, 75, 0.10), (76, 88, 0.06),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _weighted(rng, pairs, wi=1):
    """Pick a tuple from a list, by weight. `wi` is the weight's tuple index --
    STATES carries (code, name, region, weight) so it needs wi=3."""
    total = sum(p[wi] for p in pairs)
    r = rng.random() * total
    upto = 0.0
    for p in pairs:
        upto += p[wi]
        if r <= upto:
            return p
    return pairs[-1]


def _lognormal(rng, median, sigma):
    return median * math.exp(rng.gauss(0.0, sigma))


def _rand_date(rng, start, end):
    span = (end - start).days
    return start + timedelta(days=rng.randint(0, max(span, 1)))


def _sigmoid(x):
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    e = math.exp(x)
    return e / (1.0 + e)


TODAY = date(2026, 9, 1)


# ---------------------------------------------------------------------------
# Attrition (churn) label model
# ---------------------------------------------------------------------------
#
# The attrition label is generated from an explicit log-odds function so the
# planted signal is auditable and tunable. Coefficients were fitted by hand
# against tools/validate_signal.py to land the achievable AUC in the 0.80-0.86
# band: high enough that a 60-second model looks genuinely useful in front of a
# room, low enough that it does not look staged. LABEL_NOISE caps the ceiling.
#
# This label powers the OPTIONAL churn free-play leg only. The core workshop
# (medallion -> DLT -> semantic layer -> dashboard -> Genie) does not need it.

LABEL_NOISE = 0.05          # probability the final label is flipped
LABEL_INTERCEPT = 1.65      # holds the base attrition rate near 0.20

COEF = {
    "tenure": -0.46,        # per year with the bank (capped 15) -- sticky
    "log_balance": -1.15,   # per log10 dollar of total balance (centered at 3.5)
    "activity": -0.15,      # per card transaction in the window (capped 30)
    "young": 1.55,          # age < 30
    "switch_in": 0.88,      # acquired via switch_kit -- less committed
    "affluent": -1.10,      # affluent or private segment -- stickier
    "single_account": 0.82,  # only one account -- shallow relationship
}


def _attrition_logit(tenure_years, total_balance, n_txn, age, acquisition,
                     segment, n_accounts):
    z = LABEL_INTERCEPT
    z += COEF["tenure"] * min(tenure_years, 15.0)
    z += COEF["log_balance"] * (math.log10(max(total_balance, 50.0)) - 3.5)
    z += COEF["activity"] * min(n_txn, 30)
    if age < 30:
        z += COEF["young"]
    if acquisition == "switch_kit":
        z += COEF["switch_in"]
    if segment in ("affluent", "private"):
        z += COEF["affluent"]
    if n_accounts <= 1:
        z += COEF["single_account"]
    return z


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

N_CUSTOMERS = 6000
N_BRANCHES = 40
# Recent joiners (within this many days of TODAY) have no attrition outcome yet:
# they are the scoring set for the optional churn leg, mirroring "open" records.
RECENT_WINDOW_DAYS = 270


def _draw_age(rng):
    lo, hi, _ = _weighted(rng, AGE_BANDS, wi=2)
    return rng.randint(lo, hi)


def build(seed):
    """Build the datasets in memory.

    Returns (branches, customers, accounts, transactions, events).
    """
    rng = random.Random(seed)

    # -- branches ----------------------------------------------------------
    branches = []
    for i in range(N_BRANCHES):
        st = _weighted(rng, STATES, wi=3)
        branches.append({
            "branch_id": f"BR{100 + i}",
            "branch_name": f"{BRANCH_NAMES[i % len(BRANCH_NAMES)]} Branch",
            "branch_state": st[0],
            "branch_region": st[2],
            "opened_date": _rand_date(rng, date(2005, 1, 1), date(2024, 6, 30)).isoformat(),
        })

    # -- customers, accounts, transactions, events -------------------------
    customers = []
    accounts = []
    transactions = []
    events = []

    acct_seq = 700000
    txn_seq = 5000000
    evt_seq = 300000

    for i in range(N_CUSTOMERS):
        cid = f"CUST{1000000 + i}"
        st = _weighted(rng, STATES, wi=3)
        age = _draw_age(rng)
        dob = date(TODAY.year - age, rng.randint(1, 12), rng.randint(1, 28))

        seg_name, _, seg_med, seg_sigma, seg_amin, seg_amax = _weighted(rng, SEGMENTS)
        acquisition = _weighted(rng, ACQUISITION_CHANNELS)[0]
        source_bank = (
            SOURCE_BANKS[rng.randrange(len(SOURCE_BANKS))]
            if acquisition == "switch_kit" else ""
        )
        branch = branches[rng.randrange(N_BRANCHES)]

        # Tenure: switch-in customers skew newer; everyone bounded by an 18-year
        # relationship history.
        max_tenure_days = 365 * (12 if acquisition == "switch_kit" else 18)
        join_date = _rand_date(rng, TODAY - timedelta(days=max_tenure_days), TODAY)
        tenure_years = round((TODAY - join_date).days / 365.0, 2)

        # Older and higher-segment customers hold more; scale the segment median.
        age_mult = 0.6 + (age - 18) / 70.0 * 1.4
        n_accounts = rng.randint(seg_amin, seg_amax)
        total_balance = 0.0
        cust_accounts = []
        for a in range(n_accounts):
            at = _weighted(rng, ACCOUNT_TYPES)[0]
            bal = round(_lognormal(rng, seg_med * age_mult / n_accounts, seg_sigma), 2)
            open_date = _rand_date(rng, join_date, TODAY)
            cust_accounts.append({
                "account_id": f"ACC{acct_seq}",
                "customer_id": cid,
                "account_type": at,
                "open_date": open_date.isoformat(),
                "close_date": "",
                "balance": bal,
                "account_status": "open",
            })
            acct_seq += 1
            total_balance += bal
        accounts.extend(cust_accounts)

        # Card transactions. Younger customers transact more, and skew to debit
        # swipes; the count feeds the churn signal too (low activity -> churn).
        activity_base = 14.0 - (age - 18) / 70.0 * 7.0
        n_txn = max(0, int(rng.gauss(activity_base, 4.0)))
        for _ in range(n_txn):
            card_type = _weighted(rng, CARD_TYPES)[0]
            if card_type == "debit":
                # Younger -> even more likely to swipe.
                ch_table = DEBIT_CHANNELS
                if age < 30 and rng.random() < 0.25:
                    channel = "swipe"
                else:
                    channel = _weighted(rng, ch_table)[0]
            else:
                channel = _weighted(rng, CREDIT_CHANNELS)[0]
            mc = _weighted(rng, MERCHANT_CATEGORIES)
            amount = round(_lognormal(rng, mc[2], 0.6), 2)
            txn_ts = _rand_date(rng, max(join_date, TODAY - timedelta(days=365)), TODAY)
            transactions.append({
                "txn_id": f"TXN{txn_seq}",
                "account_id": cust_accounts[rng.randrange(len(cust_accounts))]["account_id"],
                "card_type": card_type,
                "channel": channel,
                "merchant_category": mc[0],
                "amount": amount,
                "txn_ts": txn_ts.isoformat(),
            })
            txn_seq += 1

        # Attrition outcome.
        is_recent = (TODAY - join_date).days < RECENT_WINDOW_DAYS
        z = _attrition_logit(tenure_years, total_balance, n_txn, age,
                             acquisition, seg_name, n_accounts)
        flag = 1 if rng.random() < _sigmoid(z) else 0
        if rng.random() < LABEL_NOISE:
            flag = 1 - flag

        if is_recent:
            status = "active" if rng.random() < 0.85 else "dormant"
            attrition_flag = None            # no outcome yet -> scoring set
            churn_date = None
        elif flag == 1:
            # A few churned customers later came back.
            if rng.random() < 0.06:
                status = "reactivated"
            else:
                status = "attrited"
            attrition_flag = 1
            churn_date = _rand_date(rng, join_date + timedelta(days=90), TODAY)
        else:
            status = "active" if rng.random() < 0.88 else "dormant"
            attrition_flag = 0
            churn_date = None

        customers.append({
            "customer_id": cid,
            "first_name": FIRST_NAMES[rng.randrange(len(FIRST_NAMES))],
            "last_name": LAST_NAMES[rng.randrange(len(LAST_NAMES))],
            "date_of_birth": dob.isoformat(),
            "gender": rng.choice(["F", "M", "F", "M", "X"]),
            "home_state": st[0],
            "home_zip": f"{rng.randint(10000, 99999)}",
            "home_branch_id": branch["branch_id"],
            "segment": seg_name,
            "acquisition_channel": acquisition,
            "source_bank": source_bank,
            "join_date": join_date.isoformat(),
            "customer_status": status,
            "attrition_flag": attrition_flag,
            "updated_at": TODAY.isoformat(),
            # carried for the generator/validator only; not written to CSV
            "_n_accounts": n_accounts,
            "_n_txn": n_txn,
            "_total_balance": round(total_balance, 2),
            "_tenure_years": tenure_years,
            "_age": age,
        })

        # -- lifecycle events for this customer ----------------------------
        events.append({
            "event_id": f"EVT{evt_seq}",
            "customer_id": cid,
            "event_type": "acquired",
            "event_date": join_date.isoformat(),
            "source_bank": source_bank,
            "reason": "switch" if acquisition == "switch_kit" else acquisition,
        })
        evt_seq += 1
        if churn_date is not None:
            events.append({
                "event_id": f"EVT{evt_seq}",
                "customer_id": cid,
                "event_type": "attrited",
                "event_date": churn_date.isoformat(),
                "source_bank": "",
                "reason": rng.choice(["fees", "rate", "service", "relocation", "competitor_offer"]),
            })
            evt_seq += 1
            if status == "reactivated":
                react = _rand_date(rng, churn_date, TODAY)
                events.append({
                    "event_id": f"EVT{evt_seq}",
                    "customer_id": cid,
                    "event_type": "reactivated",
                    "event_date": react.isoformat(),
                    "source_bank": "",
                    "reason": "win_back_offer",
                })
                evt_seq += 1

    return branches, customers, accounts, transactions, events


# ---------------------------------------------------------------------------
# Defect injection
# ---------------------------------------------------------------------------
#
# Every defect below is something the DLT/Silver leg is expected to catch with
# an expectation (quarantine) or a dedup rule. Counts are reported back so the
# facilitator can state the expected quarantine breakdown out loud.

def inject_defects(seed, customers, accounts, transactions):
    rng = random.Random(seed + 7919)
    report = {}

    state_long = {s[0]: s[1] for s in STATES}

    # 1. Long-form state names on some customers ("Tennessee" instead of "TN").
    n = 0
    for c in customers:
        if rng.random() < 0.05:
            c["home_state"] = state_long[c["home_state"]]
            n += 1
    report["long_form_state"] = n

    # 2. Alternate date format on some dates of birth: MM/DD/YYYY instead of ISO.
    n = 0
    for c in customers:
        if rng.random() < 0.05:
            d = date.fromisoformat(c["date_of_birth"])
            c["date_of_birth"] = f"{d.month:02d}/{d.day:02d}/{d.year}"
            n += 1
    report["alt_date_format_dob"] = n

    # 3. Missing date of birth -- cannot compute age, so quarantine.
    n = 0
    for c in customers:
        if rng.random() < 0.004:
            c["date_of_birth"] = ""
            n += 1
    report["missing_dob"] = n

    # 4. Negative account balance -- a data error here (overdrafts are modelled
    #    as zero), so quarantine.
    n = 0
    for a in accounts:
        if rng.random() < 0.003:
            a["balance"] = -abs(a["balance"]) - round(rng.uniform(5, 200), 2)
            n += 1
    report["negative_balance"] = n

    # 5. Balance 100x fat-finger -- implausibly high, quarantine.
    n = 0
    for a in accounts:
        if rng.random() < 0.0008:
            a["balance"] = round(a["balance"] * 100, 2)
            n += 1
    report["balance_outlier_100x"] = n

    # 6. Orphan account: customer_id that matches no customer. FK integrity.
    n = 0
    for a in accounts:
        if rng.random() < 0.002:
            a["customer_id"] = f"CUST{rng.randint(9000000, 9999999)}"
            n += 1
    report["orphan_account_customer"] = n

    # 7. Transaction amount 100x typo -- quarantine.
    n = 0
    for t in transactions:
        if rng.random() < 0.0004:
            t["amount"] = round(t["amount"] * 100, 2)
            n += 1
    report["txn_amount_outlier_100x"] = n

    # 8. Duplicate customer rows. Copies get a later updated_at so "keep the
    #    latest updated_at" is a deterministic, checkable dedup rule. The stale
    #    copy carries a different status, so picking the wrong row is visible.
    dup_custs = []
    base = rng.sample(customers, 60)
    for c in base:
        d = dict(c)
        d["updated_at"] = (date.fromisoformat(c["updated_at"])
                           - timedelta(days=rng.randint(30, 400))).isoformat()
        d["customer_status"] = rng.choice(["active", "dormant", "attrited"])
        dup_custs.append(d)
    customers.extend(dup_custs)
    report["duplicate_customer_rows"] = len(dup_custs)

    # 9. Duplicate transaction rows (exact txn_id repeated).
    dup_txns = []
    for t in rng.sample(transactions, 90):
        dup_txns.append(dict(t))
    transactions.extend(dup_txns)
    report["duplicate_txn_rows"] = len(dup_txns)

    rng.shuffle(customers)
    rng.shuffle(accounts)
    rng.shuffle(transactions)
    return report


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})


def _write_json_shards(path_prefix, rows, n_shards):
    """Newline-delimited JSON, sharded, so bronze ingestion reads a directory of
    files -- closer to how card transactions actually land, and what Auto Loader
    in the DLT pipeline expects."""
    os.makedirs(os.path.dirname(path_prefix), exist_ok=True)
    size = math.ceil(len(rows) / n_shards)
    written = []
    for s in range(n_shards):
        chunk = rows[s * size:(s + 1) * size]
        if not chunk:
            continue
        p = f"{path_prefix}_{s + 1:02d}.json"
        with open(p, "w") as fh:
            for r in chunk:
                fh.write(json.dumps(r) + "\n")
        written.append(p)
    return written


CUSTOMER_FIELDS = [
    "customer_id", "first_name", "last_name", "date_of_birth", "gender",
    "home_state", "home_zip", "home_branch_id", "segment",
    "acquisition_channel", "source_bank", "join_date", "customer_status",
    "attrition_flag", "updated_at",
]
ACCOUNT_FIELDS = [
    "account_id", "customer_id", "account_type", "open_date", "close_date",
    "balance", "account_status",
]
BRANCH_FIELDS = [
    "branch_id", "branch_name", "branch_state", "branch_region", "opened_date",
]
EVENT_FIELDS = [
    "event_id", "customer_id", "event_type", "event_date", "source_bank", "reason",
]


def generate(seed, outdir):
    os.makedirs(outdir, exist_ok=True)

    branches, customers, accounts, transactions, events = build(seed)
    defects = inject_defects(seed, customers, accounts, transactions)

    # Each source lands in its own subdirectory. Auto Loader (STREAM read_files)
    # ingests a *directory*, so one folder per entity is the clean pattern and
    # keeps the DLT pipeline's read_files paths unambiguous.
    _write_csv(os.path.join(outdir, "customers", "customers.csv"), customers, CUSTOMER_FIELDS)
    _write_csv(os.path.join(outdir, "accounts", "accounts.csv"), accounts, ACCOUNT_FIELDS)
    _write_csv(os.path.join(outdir, "branches", "branches.csv"), branches, BRANCH_FIELDS)
    _write_csv(os.path.join(outdir, "customer_events", "customer_events.csv"), events, EVENT_FIELDS)
    shards = _write_json_shards(
        os.path.join(outdir, "card_transactions", "card_transactions"), transactions, 4)

    return {
        "branches": len(branches),
        "customers": len(customers),
        "accounts": len(accounts),
        "card_transactions": len(transactions),
        "customer_events": len(events),
        "transaction_shards": len(shards),
        "defects": defects,
    }
