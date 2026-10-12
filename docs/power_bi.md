# Power BI

## Connect

**Option A: live connection to Databricks.**

1. Power BI Desktop → **Get Data → Azure Databricks / Databricks**.
2. Server hostname and HTTP path: take them from `00_platform_check` (check 7), or from **SQL Warehouses → your warehouse → Connection details**.
3. Authentication: personal access token (Databricks **Settings → Developer → Access tokens**). Never save the token in this repo.
4. Data connectivity mode: **Import**.

**Option B: CSV.** Use this if Free Edition blocks the connection or token creation.

1. Run `notebooks/09_export_gold_for_bi`.
2. Download the CSVs from **Catalog → workspace → gold → Volumes → exports**.
3. Power BI → **Get Data → Text/CSV**.
4. Note in the README that the report uses exported CSVs.

## Tables and relationships

Load `fact_order_items`, `fact_order_payments`, `v_dim_customer_current`, `dim_product`, `dim_seller`, `dim_date` and `v_orders`.

| From (many) | To (one) | Column |
|---|---|---|
| fact_order_items | dim_date | order_date_key → date_key |
| fact_order_items | dim_product | product_id |
| fact_order_items | dim_seller | seller_id |
| fact_order_items | v_dim_customer_current | customer_unique_id |
| fact_order_payments | dim_date | order_date_key → date_key |
| fact_order_payments | v_dim_customer_current | customer_unique_id |
| v_orders | dim_date | order_date_key → date_key |

All relationships use single cross-filter direction, from dimension to fact. **Do not relate the two facts to each other.**
Mark `dim_date` as the date table (column `date`).

## Measures (DAX)

```DAX
Revenue BRL = SUM ( fact_order_items[item_total_brl] )
Revenue USD = SUM ( fact_order_items[item_total_usd] )
Orders = DISTINCTCOUNT ( v_orders[order_id] )
Avg Delivery Days = AVERAGE ( v_orders[delivery_days] )
Late Delivery % = DIVIDE ( CALCULATE ( COUNTROWS ( v_orders ), v_orders[is_late] = TRUE () ),
                           CALCULATE ( COUNTROWS ( v_orders ), NOT ISBLANK ( v_orders[delivery_days] ) ) )
Payment Share % = DIVIDE ( SUM ( fact_order_payments[payment_value] ),
                           CALCULATE ( SUM ( fact_order_payments[payment_value] ), ALL ( fact_order_payments[payment_type] ) ) )
```

## Suggested page

KPI cards (Revenue USD, Orders, Avg Delivery Days, Late Delivery %), revenue by `dim_date[year_month]` as a line chart, top 10 `dim_product[category]` as a bar chart, and payment-type mix (bar chart with `Payment Share %`).
Save the `.pbix` in `dashboard/` and put a screenshot in `docs/screenshots/`.

There is also a native alternative: build the same visuals as a **Databricks AI/BI dashboard** from `dashboard/dashboard_queries.sql`.
