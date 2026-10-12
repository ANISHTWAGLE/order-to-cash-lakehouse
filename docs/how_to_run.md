# How to run on Databricks Free Edition

Every step is idempotent: if something fails, fix it and run the same step again.
Save a screenshot at each 📸 into `docs/screenshots/`.

## 1. Get the repo into the workspace

1. Push this repo to GitHub (public).
2. In Databricks: **Workspace → (your user folder) → Create → Git folder**. Paste the repo URL. Provider: GitHub.
3. Open the Git folder. You should see `notebooks/`, `src/`, `data/` and the other folders.

## 2. Platform check and setup

1. Open `notebooks/00_platform_check` and attach **Serverless** compute. Click **Run all**. 📸 the summary table.
   - Auto Loader FAIL → later, set the widget `ingest_method = copy_into` in `03_bronze_ingestion` and in the Job.
   - FX API FAIL → nothing to do; Bronze uses the committed `data/fx/` file automatically.
   - Note the SQL warehouse `server_hostname` / `http_path` for Power BI.
2. Open `00_setup_catalog_schemas` and click **Run all**. The default catalog `workspace` is the Free Edition default.
3. Upload the Olist CSVs. Go to **Catalog → workspace → bronze → Volumes → landing → raw → olist → Upload**. Upload these 7 files:
   `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`,
   `olist_customers_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`,
   `product_category_name_translation.csv`.
4. Optional: run `01_profile_olist`. 📸 the profile tables.

## 3. Run 1: initial load

1. Run `02_release_incoming_batches` with `release = initial`. This writes reference tables and months up to 2017-12.
2. Run `08_create_job`. It creates the Job `o2c_lakehouse_pipeline` and starts it.
3. Open **Jobs & Pipelines → o2c_lakehouse_pipeline → Runs**. Wait for all 5 tasks to go green. 📸 the run graph.

## 4. Run 2: idempotency (same files again)

Click **Run now** on the Job without releasing anything new.
Expected: every Bronze entity logs `0 new rows`, and Silver logs `nothing new`.

```sql
SELECT task, entity, rows_in, rows_out, message FROM workspace.ops.pipeline_run_log
WHERE run_id = (SELECT run_id FROM workspace.ops.pipeline_run_log ORDER BY started_at DESC LIMIT 1);
```
📸

## 5. Run 3: new monthly files

Run `02_release_incoming_batches` with `release = remaining`, then **Run now** on the Job.
Expected: only the 2018 files are ingested, and Silver inserts only new keys. 📸 the run log.

## 6. Run 4: simulated correction batch

Run `02_release_incoming_batches` with `release = simulated_correction`, then **Run now**.
Expected: `silver.orders` shows `rows_updated = 20` and `rows_inserted = 0`. `dim_customer` reports 5 versions closed and 5 opened. 📸 `sql/validation_queries.sql` query 8 (SCD2).

## 7. Run 5: simulated fault batch

Run `02_release_incoming_batches` with `release = simulated_faults`, then **Run now**.
Expected outcomes are in [`runbook.md`](runbook.md). 📸 `validation_queries.sql` query 9 (quarantine), and `ops.dq_results` for the run.

## 8. Failure drill (diagnose and rerun)

1. In the Job, click **Run now with different parameters** and add the task parameter `fail_on_warn = true`.
   Alternatively, edit the `dq_checks` task parameters.
   Warnings are real Olist findings, such as orders without items, so `dq_checks` goes red and the failure e-mail is sent. 📸
2. Diagnose: `SELECT * FROM workspace.ops.dq_results WHERE run_id = '<run id>' AND status = 'WARN'`. 📸
3. Set `fail_on_warn` back to `false` and click **Repair run**. Only `dq_checks` and `table_maintenance` re-run. 📸

## 9. Validation, performance, BI

- Open `sql/validation_queries.sql` in the SQL editor and run each query. 📸 queries 1, 2 and 5.
- In the Job run, open the `table_maintenance` task output. 📸 the OPTIMIZE before/after table and the EXPLAIN plan.
- Power BI: follow [`power_bi.md`](power_bi.md). If Power BI can't connect, run `09_export_gold_for_bi` and use the CSVs.

## 10. Fill in the README

Copy the real numbers from the run log, `dq_results` and the screenshots into the README **Results** table.
