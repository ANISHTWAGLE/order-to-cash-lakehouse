# Business requirements

The questions a sales/finance analyst would bring, and where the Gold layer answers them.

| # | Business question | Answered by |
|---|---|---|
| 1 | What is revenue by month, in BRL and USD, using the exchange rate on the day of the order? | `fact_order_items` × `dim_date` (`item_total_brl`, `item_total_usd`) |
| 2 | Which product categories drive revenue? | `fact_order_items` × `dim_product.category` |
| 3 | How do customers pay (card, boleto, voucher), and how many instalments? | `fact_order_payments` |
| 4 | How long does delivery take, and what share of orders arrive late, by state? | `v_orders` (`delivery_days`, `is_late`) × `v_dim_customer_current` |
| 5 | Which orders don't reconcile (paid amount ≠ items + freight)? | `v_orders.payment_mismatch`, `dq_results.payments_vs_items_mismatch` |
| 6 | Can I trust today's numbers? | `ops.dq_results` (critical checks passed) and `ops.pipeline_run_log` for the latest run |

## Non-functional requirements

- **Trust:** no row is silently dropped (quarantine plus reconciliation), and Gold totals equal Silver.
- **Re-runnable:** any task can be re-run without creating duplicates.
- **Traceable:** every row carries `_source_file` and `_load_id`. Every run is logged.
- **Documented:** data dictionary, source-to-target mapping, runbook.

## Measures for Power BI

See [`power_bi.md`](power_bi.md).
