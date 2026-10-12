# Data

## Real: Olist Brazilian E-Commerce (not committed)

- Source: Kaggle, *Brazilian E-Commerce Public Dataset by Olist*. Credit Olist wherever results are shared.
- Files used (7): `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_customers_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `product_category_name_translation.csv`.
- Not used: reviews, geolocation.
- Upload them to `/Volumes/<catalog>/bronze/landing/raw/olist/` (see `docs/how_to_run.md`). They are git-ignored.

## Real: FX rates (`fx/`)

- `fx/brl_usd_2016-08-25_2018-10-31.json` is the exact response of one Frankfurter request:
  `https://api.frankfurter.dev/v1/2016-08-25..2018-10-31?from=BRL&to=USD` (ECB reference rates, published days only).
- Bronze calls the API first and uses this file only if outbound internet is blocked. The `source` column in `bronze.fx_rates_raw` records which was used.

## Simulated (`simulated/`)

**Everything in `simulated/` is simulated, not real Olist data.** It is generated deterministically by `tools/generate_simulated_batches.py`. File names start with `simulated_`, and invented IDs start with `sim`.

| File | Purpose |
|---|---|
| `orders/simulated_correction_orders.csv` | 20 real orders whose status changes from `shipped` to `delivered` (tests MERGE updates) |
| `customers/simulated_correction_customers.csv` | 5 real customers moved to Rio de Janeiro (tests SCD2) |
| `orders/simulated_fault_orders.csv` | duplicate key, missing ID, malformed date, late-arriving 2016 order, invalid status, extra `promo_code` column |
| `orders/simulated_fault_orders_renamed_column.csv` | `order_status` renamed to `status` (schema drift) |
| `order_items/simulated_fault_order_items.csv` | negative price, orphan item, items for the simulated orders |
| `order_payments/simulated_fault_order_payments.csv` | payments for the simulated orders |
| `customers/simulated_fault_customers.csv` | customers for the simulated orders |

The expected outcome of each fault is in `docs/runbook.md`.
