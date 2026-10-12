# Runbook

## Safe rerun

Every task is idempotent, so the standard fix is to correct the cause and then **Repair run**, which re-runs only the failed task and anything after it. You can also click **Run now**.

| Task | Why a rerun is safe |
|---|---|
| bronze_ingestion | Auto Loader / COPY INTO record processed files; already-loaded files are skipped. |
| silver_transformations | Only Bronze rows newer than the entity watermark are read. MERGE updates only when the incoming row is strictly newer. |
| gold_star_schema | Facts and small dimensions are `CREATE OR REPLACE` from Silver. The SCD2 steps do nothing when no attributes changed. |
| dq_checks | Read-only, apart from appending to `ops.dq_results`. |
| table_maintenance | `OPTIMIZE` is idempotent. |

**One known non-idempotent edge case:** if Silver fails *after* MERGE but *before* the watermark update, the rerun appends that batch's quarantine rows a second time. MERGE itself stays correct. Clean it up with `DELETE FROM silver.quarantine_<entity> WHERE _quarantine_run_id = '<failed run id>'`.

**Full reset** (start again from scratch): drop the `bronze`, `silver`, `gold` and `ops` schemas with `CASCADE`, delete `landing/incoming` and `landing/checkpoints` in the Volume, then run `00_setup_catalog_schemas` again.

## Where to look

| Symptom | Look at |
|---|---|
| A task is red | Job run → task → output. Then `ops.pipeline_run_log` for that `run_id` (`status = 'FAILED'`, `message`). |
| `dq_checks` is red | `SELECT * FROM ops.dq_results WHERE run_id = '<id>' AND status IN ('FAIL','ERROR')` |
| Rows "disappeared" | `silver.quarantine_<entity>` (`failure_reason`) and the `row_reconciliation` check in `dq_results` |
| Numbers changed unexpectedly | `DESCRIBE HISTORY silver.<entity>`. Compare with `SELECT … VERSION AS OF n`. |

## Fault drills (simulated fault batch)

Release with `02_release_incoming_batches`, `release = simulated_faults`, then run the Job. All IDs start with `sim`.

| # | Fault (file) | Expected behaviour | Actual outcome | Symptom | Root cause | Fix | Validation |
|---|---|---|---|---|---|---|---|
| 1 | Duplicate `order_id` `simo…002` twice in one file (`simulated_fault_orders.csv`) | One row merged, one counted as deduplicated | `<FILL AFTER RUN>` | `duplicate_business_key` WARN, rows_failed = 1 | Source sent the same order twice | None needed: dedup is automatic | `SELECT count(*) FROM silver.orders WHERE order_id = 'simo00000000000000000000000000002'` → 1 |
| 2 | Blank `order_id` | Quarantined `missing_key` | `<FILL AFTER RUN>` | `quarantined_rows` WARN | Source row without a key | Ask the source to resend; never guess a key | `quarantine_orders` has 1 `missing_key` row |
| 3 | Purchase date `2018-13-45 99:00:00` (`simo…004`) | Quarantined `invalid_timestamp`; raw value kept | `<FILL AFTER RUN>` | `quarantined_rows` WARN | Malformed date upstream | Correct at source, re-send the row in a new file | Raw value visible in `quarantine_orders.order_purchase_timestamp` |
| 4 | Late-arriving order dated 2016-10-15 (`simo…001`) in a 2018 file | Inserted; its item gets the 2016-10-15 FX rate | `<FILL AFTER RUN>` | none | Normal late data | none | `SELECT order_date_key, brl_usd_rate FROM gold.fact_order_items WHERE order_id = 'simo00000000000000000000000000001'` |
| 5 | Status `lost_in_space` (`simo…005`) | Quarantined `invalid_status` | `<FILL AFTER RUN>` | `quarantined_rows` WARN | Unknown status value | Add it to `ALLOWED_ORDER_STATUSES` only if the business confirms it is real | `quarantine_orders` |
| 6 | Extra column `promo_code` on every row | Rows load normally; extra value kept in `_rescued_data` | `<FILL AFTER RUN>` | `schema_drift_rescued_data` WARN | Source added a column | Decide whether to add it to the contract (`src/config.py`) | `SELECT _rescued_data FROM bronze.orders WHERE _rescued_data IS NOT NULL` |
| 7 | Column renamed `order_status` → `status` (`simulated_fault_orders_renamed_column.csv`) | `order_status` arrives NULL and the row is quarantined `invalid_status`. The value is in `_rescued_data`. With `COPY INTO`, the load fails instead. | `<FILL AFTER RUN>` | `schema_drift_rescued_data` WARN + quarantine | Breaking change at the source | Agree the contract with the source; map the column if the rename is permanent | `quarantine_orders` row with `_rescued_data` containing `status` |
| 8 | Negative price on item of `simo…003` | Quarantined `negative_value` | `<FILL AFTER RUN>` | `quarantined_rows` WARN on order_items | Bad source value | Correct at source | `quarantine_order_items` |
| 9 | Item for order `simo…099` that never arrives | Quarantined `orphan_order_item` | `<FILL AFTER RUN>` | `quarantined_rows` WARN | Parent order missing | If the order arrives later, re-drop the item file under a new name | `quarantine_order_items` |

**Expected row flow for the fault batch.** orders: 8 in = 3 merged + 4 quarantined + 1 deduplicated. order_items: 4 in = 2 merged + 2 quarantined. order_payments: 2 merged. customers: 6 merged.

Which copy of the duplicate is kept (fault 1) is decided by a hash of the row. Both copies share the same file and ingestion time, so there is no "later" one. The choice is the same on every rerun.

## Correction batch

`release = simulated_correction`: 20 orders change from `shipped` to `delivered`, and 5 customers move to Rio de Janeiro.

| Expected | Actual |
|---|---|
| `silver.orders`: rows_updated = 20, rows_inserted = 0 | **20 updated, 0 inserted** (all 20 now `delivered`) |
| `silver.customers`: rows_updated = 5 | **5 updated, 0 inserted** |
| `dim_customer`: 5 versions closed, 5 opened, still exactly one current row per customer | **96,200 versions, 96,096 current, 5 closed** |
