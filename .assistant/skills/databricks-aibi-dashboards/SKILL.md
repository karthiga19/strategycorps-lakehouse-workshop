---
name: databricks-aibi-dashboards
description: Databricks AI/BI dashboard patterns — .lvdash.json structure, the 6-column grid, widget specs and versions (counter, bar, line, table, choropleth map, filter), number formatting, and deployment via the Workspace API. Use when creating, editing, or deploying an AI/BI dashboard.
metadata:
  adapted_from: databricks-solutions/vibe-coding-workshop-template
  adapted_for: banking gold Delta tables and a metric view as datasets
  domain: monitoring
---

# Databricks AI/BI dashboards

A dashboard is a single `.lvdash.json` file: a list of **datasets** (named SQL
queries) and a list of **pages** holding **widgets** that reference those
datasets by name. Get the structure right and it renders; get a `fieldName`
wrong and the widget silently shows nothing.

## Top-level structure

```json
{
  "datasets": [
    {
      "name": "ds_kpi_totals",
      "displayName": "KPI totals",
      "queryLines": ["SELECT COUNT(*) AS total_customers, ", "SUM(total_balance) AS total_balance ", "FROM catalog.schema.gold_customer_360"]
    }
  ],
  "pages": [
    {"name": "page_main", "displayName": "Overview", "layout": [ /* widgets */ ]}
  ]
}
```

`queryLines` is an array of strings that get concatenated — **each element must
end with a space or newline**, or tokens weld together into invalid SQL.

## Critical rules

| # | Rule | Failure if broken |
|---|---|---|
| 1 | **6-column grid, not 12.** `x` is 0–5; widths per row sum to ≤ 6. | Widgets snap to strange positions. |
| 2 | **`fieldName` must exactly match the query's output alias.** | Widget renders empty, no error. |
| 3 | **Return raw numbers; let the widget format them.** Rates as `0.78`, not `"78%"`. | A formatted string cannot be charted. |
| 4 | **Widget versions**: counter = 2, table = 2, filter = 2, bar/line/pie/area/scatter = 3, pivot = 3, choropleth map = 1, symbol map = 2. | Wrong version silently fails. |
| 5 | **Every dataset a widget references must exist** in `datasets`. | Load error on the page. |
| 6 | **No `period` in a counter's encodings.** | Counter fails to render. |
| 7 | **Two-letter state codes for a US choropleth.** | Regions do not match; map is blank. |

## Grid patterns

```
KPI row, 4 counters      {"x":0,"y":0,"width":1,"height":2} ... x:1, x:2, x:3
Two side-by-side charts  {"x":0,"y":2,"width":3,"height":6} and {"x":3,"y":2,...}
Full-width chart/table   {"x":0,"y":8,"width":6,"height":6}
```

Typical heights: filters 1–2, counters 2, charts 6, large charts 9, tables 6+.

## Widget specs

### Counter (version 2)

```json
{
  "widget": {
    "name": "kpi_total_customers",
    "queries": [{"name": "main_query", "query": {
      "datasetName": "ds_kpi_totals",
      "fields": [{"name": "total_customers", "expression": "`total_customers`"}],
      "disaggregated": false
    }}],
    "spec": {
      "version": 2, "widgetType": "counter",
      "encodings": {"value": {
        "fieldName": "total_customers", "displayName": "Total customers",
        "format": {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 0}}
      }},
      "frame": {"showTitle": true, "title": "Total customers"}
    }
  },
  "position": {"x": 0, "y": 0, "width": 1, "height": 2}
}
```

Currency: `{"type": "number-currency", "currencyCode": "USD", "decimalPlaces": {"type": "exact", "places": 0}}`
Percent: `{"type": "number-percent", "decimalPlaces": {"type": "exact", "places": 1}}` — with a query that returns a 0–1 decimal (rule 3).

### Bar chart (version 3)

```json
{
  "widget": {
    "name": "chart_balance_by_age",
    "queries": [{"name": "main_query", "query": {
      "datasetName": "ds_balance_by_age",
      "fields": [
        {"name": "age_band", "expression": "`age_band`"},
        {"name": "avg_balance", "expression": "`avg_balance`"}
      ],
      "disaggregated": false
    }}],
    "spec": {
      "version": 3, "widgetType": "bar",
      "encodings": {
        "x": {"fieldName": "age_band", "displayName": "Age band", "scale": {"type": "categorical"}},
        "y": {"fieldName": "avg_balance", "displayName": "Avg balance", "scale": {"type": "quantitative"}}
      },
      "frame": {"showTitle": true, "title": "Average balance by age band"}
    }
  },
  "position": {"x": 0, "y": 2, "width": 3, "height": 6}
}
```

Add a `color` encoding to split each bar by a second dimension; omit it for a plain bar.

### Choropleth map (version 1)

```json
{
  "widget": {
    "name": "map_customers",
    "queries": [{"name": "main_query", "query": {
      "datasetName": "ds_by_state",
      "fields": [
        {"name": "home_state", "expression": "`home_state`"},
        {"name": "customer_count", "expression": "`customer_count`"}
      ],
      "disaggregated": false
    }}],
    "spec": {
      "version": 1, "widgetType": "choropleth-map",
      "encodings": {
        "location": {"fieldName": "home_state", "scale": {"type": "categorical", "mapType": "us-states"}},
        "color": {"fieldName": "customer_count", "displayName": "Customers", "scale": {"type": "quantitative"}}
      },
      "frame": {"showTitle": true, "title": "Customers by state"}
    }
  },
  "position": {"x": 3, "y": 2, "width": 3, "height": 6}
}
```

### Table (version 2)

```json
{
  "widget": {
    "name": "table_retention",
    "queries": [{"name": "main_query", "query": {
      "datasetName": "ds_retention",
      "fields": [
        {"name": "source_bank", "expression": "`source_bank`"},
        {"name": "retention_rate", "expression": "`retention_rate`"}
      ],
      "disaggregated": true
    }}],
    "spec": {
      "version": 2, "widgetType": "table",
      "encodings": {"columns": [
        {"fieldName": "source_bank", "displayName": "Source bank"},
        {"fieldName": "retention_rate", "displayName": "Retention",
         "format": {"type": "number-percent", "decimalPlaces": {"type": "exact", "places": 1}}}
      ]},
      "frame": {"showTitle": true, "title": "Switch-in retention by source bank"}
    }
  },
  "position": {"x": 0, "y": 8, "width": 6, "height": 6}
}
```

Note `"disaggregated": true` for tables — they show rows, not aggregates.

## Deployment

```python
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient()
path = f"/Users/{w.current_user.me().user_name}/Strategy Corps — Retail Banking Overview.lvdash.json"
w.workspace.upload(path, json.dumps(dashboard).encode("utf-8"),
                   format=ImportFormat.AUTO, overwrite=True)
```

`ImportFormat.AUTO` picks the dashboard type from the `.lvdash.json` extension —
the extension is required.

## Before declaring it done

1. Every widget's `datasetName` exists in `datasets`.
2. Every `fieldName` matches an alias the dataset's SQL actually produces.
3. Widths sum to ≤ 6 on every row.
4. Versions match rule 4; state codes are 2-letter (rule 7).
5. Each `queryLines` element ends with whitespace.
6. Open the dashboard and confirm every widget shows data — an empty widget is
   almost always rule 2.
