# Data dictionary (Gold)

Money is `DECIMAL(12,2)` in BRL unless the column name ends in `_usd`. FX rates are `DECIMAL(12,6)`.

## fact_order_items: one row per order item (`order_id`, `order_item_id`)

| Column | Type | Description |
|---|---|---|
| order_id | STRING | Order identifier |
| order_item_id | INT | Line number within the order (1, 2, …) |
| product_id | STRING | → `dim_product` |
| seller_id | STRING | → `dim_seller` |
| customer_unique_id | STRING | → `v_dim_customer_current` (the person, not the per-order `customer_id`) |
| order_date_key | INT | Purchase date `yyyymmdd` → `dim_date.date_key`. Liquid clustering key |
| order_status | STRING | Status of the parent order |
| price | DECIMAL | Item price, BRL |
| freight_value | DECIMAL | Freight for this item, BRL |
| item_total_brl | DECIMAL | price + freight_value |
| brl_usd_rate | DECIMAL | ECB BRL→USD rate on the purchase date (forward-filled) |
| price_usd, freight_usd, item_total_usd | DECIMAL | Amounts converted at `brl_usd_rate`, rounded to 2 dp |

## fact_order_payments: one row per payment record (`order_id`, `payment_sequential`)

| Column | Type | Description |
|---|---|---|
| order_id | STRING | Order identifier |
| payment_sequential | INT | Payment number within the order. One order can be paid in several parts, e.g. voucher + card |
| payment_type | STRING | `credit_card`, `boleto`, `voucher`, `debit_card`, `not_defined` |
| payment_installments | INT | Number of instalments |
| payment_value | DECIMAL | Amount paid, BRL. May differ from item totals (vouchers, instalment interest) |
| customer_unique_id | STRING | → `v_dim_customer_current` |
| order_date_key | INT | → `dim_date` |
| brl_usd_rate | DECIMAL | Rate on the purchase date |
| payment_value_usd | DECIMAL | Converted amount |

## dim_customer: one row per customer version (SCD Type 2)

| Column | Type | Description |
|---|---|---|
| customer_sk | BIGINT | Surrogate key of the version (`xxhash64(customer_unique_id, valid_from)`) |
| customer_unique_id | STRING | Natural key: the person |
| customer_zip_code_prefix, customer_city, customer_state | STRING | Tracked attributes: the address of the person's latest order |
| attr_hash | STRING | SHA-256 of the tracked attributes, used to detect changes |
| valid_from / valid_to | TIMESTAMP | Validity window. `valid_to` is NULL for the current version |
| is_current | BOOLEAN | Exactly one TRUE per `customer_unique_id` (checked in `dq_results`) |

`v_dim_customer_current` is the current version only. Use it for BI relationships.

## dim_product: one row per product

| Column | Description |
|---|---|
| product_id | Natural key |
| category | English category. Falls back to the Portuguese name if untranslated, or `unknown` if blank |
| category_pt | Original Portuguese category |
| category_translation_missing | TRUE when a category exists but has no English translation |
| product_name_length, product_description_length | Renamed from the misspelled source columns (`…_lenght`) |
| product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm | Physical attributes |

## dim_seller, dim_date, fx_rate_daily

| Table | Columns |
|---|---|
| dim_seller | seller_id, seller_zip_code_prefix, seller_city, seller_state |
| dim_date | date_key (INT yyyymmdd), date, year, quarter, month, month_name, year_month, day_of_week (1 = Sunday), day_name, is_weekend |
| fx_rate_daily | rate_date, brl_usd_rate, is_filled (TRUE = weekend/holiday, carried forward) |

## v_orders: one row per order

order_id, customer_unique_id, order_status, order_date_key, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date, **delivery_days** (NULL if not delivered), **is_late** (delivered after the estimate), item_count, items_total_brl, payment_count, payments_total_brl, **payment_mismatch** (|items − payments| > 0.01).

## ops

| Table | Grain |
|---|---|
| pipeline_run_log | one row per task (and entity) per run: rows in/out/quarantined/deduplicated/inserted/updated, start, end, status, message |
| dq_results | one row per check per run: check, table, severity, rows checked, rows failed, status, details |
| silver_watermarks | one row per entity: highest Bronze `_ingested_at` already processed |
