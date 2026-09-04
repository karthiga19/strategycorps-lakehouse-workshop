"""Check the planted attrition signal without needing sklearn or Spark.

Fits a plain logistic regression (batch gradient descent, stdlib only) on the
same features the optional churn leg uses, and reports held-out AUC plus the
label base rate.

A stdlib logistic regression is a *lower* bound on what a gradient-boosted model
will achieve in the workshop, since it cannot model the interactions or the
clipping in the label function. Aim for logistic AUC around 0.78-0.85; the
boosted model in the notebook should land a little above it.

    python3 tools/validate_signal.py [seed]
"""

import math
import os
import random
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "setup"))

import _generator as gen  # noqa: E402


FEATURES = [
    "tenure_years",
    "log_balance",
    "txn_activity",
    "is_young",
    "is_switch_in",
    "is_affluent",
    "single_account",
]


def featurize(customers):
    rows = []
    for c in customers:
        if c["attrition_flag"] is None:
            continue  # recent joiners are the scoring set, not training data
        rows.append((
            [
                min(float(c["_tenure_years"]), 15.0),
                math.log10(max(float(c["_total_balance"]), 50.0)) - 3.5,
                min(int(c["_n_txn"]), 30),
                1.0 if int(c["_age"]) < 30 else 0.0,
                1.0 if c["acquisition_channel"] == "switch_kit" else 0.0,
                1.0 if c["segment"] in ("affluent", "private") else 0.0,
                1.0 if int(c["_n_accounts"]) <= 1 else 0.0,
            ],
            int(c["attrition_flag"]),
        ))
    return rows


def standardize(train, test):
    n = len(FEATURES)
    mean = [0.0] * n
    for x, _ in train:
        for j in range(n):
            mean[j] += x[j]
    mean = [m / len(train) for m in mean]
    var = [0.0] * n
    for x, _ in train:
        for j in range(n):
            var[j] += (x[j] - mean[j]) ** 2
    sd = [math.sqrt(v / len(train)) or 1.0 for v in var]

    def apply(rows):
        return [([(x[j] - mean[j]) / sd[j] for j in range(n)], y) for x, y in rows]

    return apply(train), apply(test)


def fit(rows, epochs=400, lr=0.35, l2=1e-4):
    n = len(FEATURES)
    w = [0.0] * n
    b = 0.0
    m = len(rows)
    for _ in range(epochs):
        gw = [0.0] * n
        gb = 0.0
        for x, y in rows:
            z = b + sum(w[j] * x[j] for j in range(n))
            p = 1.0 / (1.0 + math.exp(-z)) if z > -700 else 0.0
            e = p - y
            for j in range(n):
                gw[j] += e * x[j]
            gb += e
        for j in range(n):
            w[j] -= lr * (gw[j] / m + l2 * w[j])
        b -= lr * (gb / m)
    return w, b


def auc(scores, labels):
    pairs = sorted(zip(scores, labels))
    pos = sum(labels)
    neg = len(labels) - pos
    if pos == 0 or neg == 0:
        return float("nan")
    ranks = [0.0] * len(pairs)
    i = 0
    while i < len(pairs):
        j = i
        while j + 1 < len(pairs) and pairs[j + 1][0] == pairs[i][0]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    rank_sum = sum(r for r, (_, y) in zip(ranks, pairs) if y == 1)
    return (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg)


def main():
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 12345

    branches, customers, accounts, transactions, events = gen.build(seed)
    gen.inject_defects(seed, customers, accounts, transactions)

    rows = featurize(customers)
    rng = random.Random(seed)
    rng.shuffle(rows)
    cut = int(len(rows) * 0.75)
    train, test = rows[:cut], rows[cut:]
    train_s, test_s = standardize(train, test)

    w, b = fit(train_s)
    scores = [b + sum(w[j] * x[j] for j in range(len(FEATURES))) for x, _ in test_s]
    labels = [y for _, y in test_s]

    base = sum(y for _, y in rows) / len(rows)
    a = auc(scores, labels)

    print(f"seed                 : {seed}")
    print(f"labelled customers   : {len(rows)}")
    print(f"train / test         : {len(train)} / {len(test)}")
    print(f"label base rate      : {base:.3f}")
    print(f"held-out AUC (logreg): {a:.4f}")
    print()
    print("coefficients (standardized):")
    for name, coef in sorted(zip(FEATURES, w), key=lambda t: -abs(t[1])):
        print(f"  {name:20s} {coef:+.3f}")
    print()
    lo, hi = 0.75, 0.85
    verdict = "OK" if lo <= a <= hi else "OUT OF BAND -- retune"
    print(f"target logreg band {lo}-{hi} -> {verdict}")


if __name__ == "__main__":
    main()
