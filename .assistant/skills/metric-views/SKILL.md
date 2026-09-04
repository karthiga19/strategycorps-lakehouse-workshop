---
name: metric-views
description: Unity Catalog metric views — defining governed business metrics in YAML (CREATE ... WITH METRICS LANGUAGE YAML), dimensions vs measures, the MEASURE() query function, and UC tagging/governance. Use when building a semantic layer / metric view over a gold table.
metadata:
  domain: semantic-layer
---

# Unity Catalog metric views

A metric view is a governed YAML definition of dimensions and measures over a
source table. Aggregation is **deferred to query time**, so a ratio like a
retention rate re-aggregates correctly at any grouping — the whole reason to use
one instead of a plain view.

## Create

```sql
CREATE OR REPLACE VIEW catalog.schema.mv_banking_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: catalog.schema.gold_customer_360
  comment: "Governed retail-banking metrics."
  dimensions:
    - name: Age
      expr: age
    - name: Age Band
      expr: age_band
    - name: Segment
      expr: segment
    - name: Moved From Bank
      expr: moved_from_bank
  measures:
    - name: Customer Count
      expr: COUNT(1)
    - name: Average Balance
      expr: AVG(total_balance)
    - name: Debit Card Swipes
      expr: SUM(debit_swipe_count)
    - name: Retention Rate
      expr: AVG(CASE WHEN is_retained THEN 1.0 ELSE 0.0 END)
$$;
```

- `version: 1.1` (Databricks Runtime / serverless SQL 17.2+).
- **Dimensions** are any SQL expression used for grouping/filtering; keep a
  numeric column like `age` as a dimension so `BETWEEN 61 AND 65` works.
- **Measures** must be aggregate expressions.
- Names may contain spaces; they are backtick-quoted in queries.

## Query

```sql
SELECT `Age Band`, MEASURE(`Average Balance`) AS avg_balance
FROM catalog.schema.mv_banking_metrics
WHERE `Age` BETWEEN 61 AND 65
GROUP BY ALL
ORDER BY ALL;
```

**Rules that trip people up:**

| Rule | Why |
|---|---|
| Every measure is wrapped in `MEASURE(\`name\`)` | Aggregation happens at query time |
| `SELECT *` is not supported | You must list dimensions + MEASURE() calls |
| Names with spaces need backticks | `\`Average Balance\`` |
| Joins go in the YAML `joins:` block, not the query | Query-time joins fail |

## Govern it

```sql
COMMENT ON VIEW catalog.schema.mv_banking_metrics IS 'Certified retail-banking metrics.';
ALTER VIEW catalog.schema.mv_banking_metrics SET TAGS ('domain' = 'retail_banking', 'certified' = 'true');
```

Tags make the view discoverable in Catalog Explorer and signal it as the
trusted definition. Genie reads metric views natively, so tagging + a clear
`comment` on each measure is what makes the agent answer with the right numbers.

## Before declaring it done

1. It queries: run one `MEASURE()` query per headline metric.
2. A numeric dimension exists for any "between X and Y" question.
3. The view has a `comment` and discoverability tags.
4. Filtering to `Moved From Bank = true` gives switch-in retention from the same
   `Retention Rate` measure — proof the ratio re-aggregates.
