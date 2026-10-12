# Source-to-target mapping

Transformation rules from the Olist source columns to Silver and Gold. "Trim" means leading and trailing spaces are removed and an empty string becomes NULL.

## orders → silver.orders → gold (facts, v_orders)

| Source column | Silver column | Rule | Gold |
|---|---|---|---|
| order_id | order_id | Trim. Blank → `missing_key` | fact_*.order_id, v_orders |
| customer_id | customer_id | Trim. Blank → `missing_key` | Resolved to `customer_unique_id` through silver.customers |
| order_status | order_status | Trim, lowercase. Must be in the allowed list, otherwise `invalid_status` | fact_order_items.order_status, v_orders |
| order_purchase_timestamp | order_purchase_timestamp | `try_to_timestamp('yyyy-MM-dd HH:mm:ss')`. Required. Unparseable → `invalid_timestamp` | order_date_key = `yyyyMMdd`. Also selects the FX rate |
| order_approved_at, order_delivered_carrier_date | same | Parsed; blank allowed | (not used) |
| order_delivered_customer_date | same | Parsed; blank allowed | v_orders.delivery_days, is_late |
| order_estimated_delivery_date | same | Parsed; blank allowed | v_orders.is_late |

## order_items → silver.order_items → gold.fact_order_items

| Source | Silver | Rule | Gold |
|---|---|---|---|
| order_id | order_id | Required. Must exist in silver.orders, otherwise `orphan_order_item` | order_id |
| order_item_id | order_item_id INT | Via DOUBLE → INT. Required | order_item_id |
| product_id, seller_id | same | Required | product_id, seller_id |
| shipping_limit_date | TIMESTAMP | Parsed | (not used) |
| price, freight_value | DECIMAL(12,2) | `try_cast`. Unparseable → `invalid_number`. < 0 → `negative_value` | price, freight_value, item_total_brl, *_usd |

## order_payments → silver.order_payments → gold.fact_order_payments

| Source | Silver | Rule | Gold |
|---|---|---|---|
| order_id | order_id | Required; orphan → `orphan_order_payment` | order_id |
| payment_sequential | INT | Required | payment_sequential |
| payment_type | STRING | Trim, lowercase | payment_type |
| payment_installments | INT | ≥ 0 | payment_installments |
| payment_value | DECIMAL(12,2) | ≥ 0 | payment_value, payment_value_usd |

## customers → silver.customers → gold.dim_customer (SCD2)

| Source | Silver | Rule | Gold |
|---|---|---|---|
| customer_id | customer_id | Key (one per order) | not in the dimension; used to link orders to the person |
| customer_unique_id | same | Required | dim_customer natural key |
| customer_zip_code_prefix | same | Trim; kept as STRING to preserve leading zeros | tracked attribute |
| customer_city | same | Trim, lowercase | tracked attribute |
| customer_state | same | Trim, uppercase | tracked attribute |

Tracked attributes come from the person's **latest order**. A change in their hash closes the current version and opens a new one.

## products (+ category_translation) → silver.products → gold.dim_product

| Source | Silver | Rule | Gold |
|---|---|---|---|
| product_id | product_id | Key | product_id |
| product_category_name | same | Trim, lowercase | category_pt |
| (translation file) | product_category_name_english | Left join on the Portuguese name. Fallback: Portuguese name, then `unknown` | category |
| (derived) | category_translation_missing | Category present but no translation | category_translation_missing |
| product_name_lenght | product_name_length | **Renamed** (source typo). INT | product_name_length |
| product_description_lenght | product_description_length | **Renamed**. INT | product_description_length |
| product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm | INT | ≥ 0 | same |

## sellers → silver.sellers → gold.dim_seller

seller_id (key), seller_zip_code_prefix, seller_city (lowercase), seller_state (uppercase). All are carried through unchanged.

## Frankfurter JSON → bronze.fx_rates_raw → silver.fx_rates → gold.fx_rate_daily

| Source | Bronze | Silver | Gold |
|---|---|---|---|
| Whole response | `payload` (raw JSON string) + `source` (`frankfurter_api` / `repo_fallback_file`) | `from_json` → `explode(rates)` | — |
| `rates.<date>.USD` | — | rate_date DATE, brl_usd_rate DECIMAL(12,6) (published ECB days only) | Full calendar, forward-filled; `is_filled` marks carried days |
