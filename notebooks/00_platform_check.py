# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Platform check
# MAGIC Tests what **this** workspace can actually do, so later phases don't assume features.
# MAGIC Run all cells top to bottom (serverless compute is fine). It creates a scratch schema
# MAGIC (`_platform_check`) and removes it at the end.
# MAGIC
# MAGIC | # | Check | If it FAILS |
# MAGIC |---|---|---|
# MAGIC | 1 | Environment info | informational only |
# MAGIC | 2 | Unity Catalog: create schema + Delta table | wrong catalog name? Pick one from `SHOW CATALOGS` and re-run with the widget |
# MAGIC | 3 | Volume for landing files | no Volumes: use a workspace/DBFS path as the landing zone (tell me) |
# MAGIC | 4a | Auto Loader (`cloudFiles`), re-run adds no duplicates | use `COPY INTO` (check 4b) |
# MAGIC | 4b | `COPY INTO`, re-run adds no duplicates | ask me; fallback is a manual "processed files" log table |
# MAGIC | 5 | Outbound call to the FX API | upload the FX JSON as a file instead |
# MAGIC | 6 | Job with task dependency (created, run, deleted by this notebook) | build the Job by hand in the UI; see manual steps at the bottom |
# MAGIC | 7 | SQL warehouse details for Power BI | export Gold to CSV instead |

# COMMAND ----------

import json
import re
import time
import urllib.error
import urllib.request
from datetime import timedelta

dbutils.widgets.text("catalog", "workspace", "Catalog to test in")
dbutils.widgets.text("probe_schema", "_platform_check", "Scratch schema")
dbutils.widgets.dropdown("run_job_probe", "yes", ["yes", "no"], "Run the Jobs probe (check 6)")
dbutils.widgets.dropdown("cleanup", "yes", ["yes", "no"], "Drop scratch schema at the end")
dbutils.widgets.text("run_mode", "full", "Internal: 'noop' when started by the Jobs probe")

# When the Jobs probe runs this same notebook as a task, do nothing and succeed.
if dbutils.widgets.get("run_mode") == "noop":
    dbutils.notebook.exit("noop ok")

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ident(name):
    if not IDENT.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


CATALOG = ident(dbutils.widgets.get("catalog"))
SCHEMA = ident(dbutils.widgets.get("probe_schema"))
VOLUME = "landing_probe"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

results = []  # (check, status, detail, fallback)


def run_check(name, fn, fallback=""):
    """Run one check; never stop the notebook, always record PASS/FAIL."""
    try:
        detail = fn() or ""
        results.append((name, "PASS", str(detail), ""))
        print(f"[PASS] {name}: {detail}")
    except Exception as e:  # noqa: BLE001 - we want every failure recorded
        detail = f"{type(e).__name__}: {str(e)[:400]}"
        results.append((name, "FAIL", detail, fallback))
        print(f"[FAIL] {name}: {detail}\n       fallback: {fallback}")


# COMMAND ----------

# MAGIC %md ## 1. Environment

# COMMAND ----------


def check_env():
    user = spark.sql("SELECT current_user()").first()[0]
    catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    return f"spark={spark.version}; user={user}; catalogs={catalogs}"


run_check("1 Environment", check_env)

# COMMAND ----------

# MAGIC %md ## 2. Unity Catalog: schema + Delta table

# COMMAND ----------


def check_uc():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
    t = f"`{CATALOG}`.`{SCHEMA}`.`uc_probe`"
    spark.sql(f"CREATE OR REPLACE TABLE {t} (id INT, v STRING) USING DELTA")
    spark.sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b')")
    n = spark.table(t).count()
    fmt = spark.sql(f"DESCRIBE DETAIL {t}").first()["format"]
    assert n == 2 and fmt == "delta", f"rows={n}, format={fmt}"
    return f"created {CATALOG}.{SCHEMA}.uc_probe, rows={n}, format={fmt}"


run_check("2 Unity Catalog table", check_uc, "check the catalog widget against SHOW CATALOGS")

# COMMAND ----------

# MAGIC %md ## 3. Volume for landing files

# COMMAND ----------


def write_text(path, text):
    with open(path, "w") as f:
        f.write(text)


def check_volume():
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
    dbutils.fs.mkdirs(f"{VOL}/probe")
    write_text(f"{VOL}/probe/hello.txt", "hello")
    with open(f"{VOL}/probe/hello.txt") as f:
        assert f.read() == "hello"
    names = [x.name for x in dbutils.fs.ls(f"{VOL}/probe")]
    return f"wrote and read {VOL}/probe/hello.txt; ls={names}"


run_check("3 Volume", check_volume, "no Volumes: use another landing path and tell me")

# COMMAND ----------

# MAGIC %md ## 4a. Auto Loader (idempotent re-run)
# MAGIC Run 1 ingests 2 files, run 2 must add nothing, run 3 (after a new file) adds only the new file.

# COMMAND ----------


def make_csv(folder, name, rows):
    dbutils.fs.mkdirs(folder)
    write_text(f"{folder}/{name}", "id,v\n" + "\n".join(f"{i},{v}" for i, v in rows) + "\n")


def check_autoloader():
    src = f"{VOL}/al_src"
    ckpt = f"{VOL}/al_ckpt"
    schema_loc = f"{VOL}/al_schema"
    target = f"`{CATALOG}`.`{SCHEMA}`.`al_target`"
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    for p in (src, ckpt, schema_loc):
        dbutils.fs.rm(p, True)
    make_csv(src, "f1.csv", [(1, "a"), (2, "b"), (3, "c")])
    make_csv(src, "f2.csv", [(4, "d"), (5, "e")])

    def run_once():
        (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", schema_loc)
            .load(src)
            .writeStream.option("checkpointLocation", ckpt)
            .trigger(availableNow=True)
            .toTable(target)
            .awaitTermination()
        )
        return spark.table(target).count()

    n1 = run_once()
    n2 = run_once()
    make_csv(src, "f3.csv", [(6, "f")])
    n3 = run_once()
    assert (n1, n2, n3) == (5, 5, 6), f"counts after run1/run2/run3 = {(n1, n2, n3)}, expected (5, 5, 6)"
    return f"counts run1/run2/run3 = {n1}/{n2}/{n3} (expected 5/5/6): idempotent"


run_check("4a Auto Loader", check_autoloader, "use COPY INTO (check 4b) for Bronze")

# COMMAND ----------

# MAGIC %md ## 4b. COPY INTO (fallback, also idempotent)

# COMMAND ----------


def check_copy_into():
    src = f"{VOL}/ci_src"
    target = f"`{CATALOG}`.`{SCHEMA}`.`ci_target`"
    dbutils.fs.rm(src, True)
    make_csv(src, "f1.csv", [(1, "a"), (2, "b"), (3, "c")])
    make_csv(src, "f2.csv", [(4, "d"), (5, "e")])
    spark.sql(f"CREATE OR REPLACE TABLE {target} (id STRING, v STRING)")
    sql = (
        f"COPY INTO {target} FROM '{src}' FILEFORMAT = CSV "
        "FORMAT_OPTIONS ('header' = 'true')"
    )
    spark.sql(sql)
    n1 = spark.table(target).count()
    spark.sql(sql)
    n2 = spark.table(target).count()
    make_csv(src, "f3.csv", [(6, "f")])
    spark.sql(sql)
    n3 = spark.table(target).count()
    assert (n1, n2, n3) == (5, 5, 6), f"counts = {(n1, n2, n3)}, expected (5, 5, 6)"
    return f"counts run1/run2/run3 = {n1}/{n2}/{n3} (expected 5/5/6): idempotent"


run_check("4b COPY INTO", check_copy_into, "ask me for a manual processed-files log approach")

# COMMAND ----------

# MAGIC %md ## 5. Outbound call to the FX API (Frankfurter)
# MAGIC The current Frankfurter docs describe `/v2/rates` (v1 is deprecated but documented as still available).
# MAGIC The check tries **one date-range request** on v2, then v1, and reports the response shape and row count.
# MAGIC If the count is ~110 instead of ~560, the API is returning weekly data, not daily; tell me.

# COMMAND ----------

FX_V2 = (
    "https://api.frankfurter.dev/v2/rates"
    "?from=2016-09-01&to=2018-10-31&base=BRL&quotes=USD&providers=ecb"
)
FX_V1 = "https://api.frankfurter.dev/v1/2016-09-01..2018-10-31?base=BRL&symbols=USD"


def http_get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "o2c-lakehouse-platform-check"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, json.loads(r.read().decode("utf-8"))


def describe_fx(body):
    """Describe the JSON without assuming its shape."""
    if isinstance(body, list):
        return f"list of {len(body)} rows; first={body[0] if body else None}; last={body[-1] if body else None}"
    if isinstance(body, dict) and isinstance(body.get("rates"), dict):
        days = sorted(body["rates"])
        return f"dict with rates for {len(days)} dates; first={days[0]}; last={days[-1]}"
    return f"unrecognised shape: {str(body)[:200]}"


def check_fx():
    notes = []
    for label, url in (("v2", FX_V2), ("v1", FX_V1)):
        try:
            status, body = http_get_json(url)
            return f"{label} OK (HTTP {status}); " + describe_fx(body) + (f" | earlier: {notes}" if notes else "")
        except urllib.error.HTTPError as e:
            notes.append(f"{label}: HTTP {e.code}")
        except Exception as e:  # noqa: BLE001 - e.g. URLError when egress is blocked
            notes.append(f"{label}: {type(e).__name__}: {str(e)[:150]}")
    raise RuntimeError("both endpoints failed: " + "; ".join(notes))


run_check("5 Outbound FX API", check_fx, "upload the FX JSON as a file to the Volume instead")

# COMMAND ----------

# MAGIC %md ## 6. Jobs: two tasks with a dependency + failure e-mail setting
# MAGIC Creates a temporary Job (task_b depends on task_a, both run this notebook in `noop` mode),
# MAGIC runs it, reports the result, then deletes it. Set the widget `run_job_probe` to `no` to skip.

# COMMAND ----------


def check_jobs():
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs

    w = WorkspaceClient()
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    email = spark.sql("SELECT current_user()").first()[0]

    def task(key, depends_on=None):
        return jobs.Task(
            task_key=key,
            depends_on=[jobs.TaskDependency(task_key=d) for d in (depends_on or [])],
            notebook_task=jobs.NotebookTask(notebook_path=nb_path, base_parameters={"run_mode": "noop"}),
        )

    created = w.jobs.create(
        name="_platform_check_probe",
        tasks=[task("task_a"), task("task_b", ["task_a"])],
        email_notifications=jobs.JobEmailNotifications(on_failure=[email]),
    )
    try:
        run = w.jobs.run_now(job_id=created.job_id).result(timeout=timedelta(minutes=10))
        per_task = {t.task_key: str(t.state.result_state) for t in run.tasks}
        overall = str(run.state.result_state)
        assert "SUCCESS" in overall, f"run result {overall}, tasks {per_task}"
        return f"job created+run OK; overall={overall}; tasks={per_task}; on_failure e-mail set to {email} (delivery not tested)"
    finally:
        w.jobs.delete(job_id=created.job_id)


if dbutils.widgets.get("run_job_probe") == "yes":
    run_check("6 Jobs with task dependency", check_jobs, "create the Job by hand in the UI (manual steps below)")
else:
    results.append(("6 Jobs with task dependency", "SKIPPED", "run_job_probe = no", ""))

# COMMAND ----------

# MAGIC %md ## 7. SQL warehouse details (for Power BI)

# COMMAND ----------


def check_warehouses():
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    lines = []
    for wh in w.warehouses.list():
        d = w.warehouses.get(wh.id)
        p = d.odbc_params
        lines.append(
            f"{d.name} (state={d.state}, type={d.warehouse_type}); "
            f"server_hostname={p.hostname if p else '?'}; http_path={p.path if p else '?'}"
        )
    assert lines, "no SQL warehouses visible to this user"
    return " || ".join(lines)


run_check("7 SQL warehouse for Power BI", check_warehouses, "export Gold tables to CSV for Power BI")

# COMMAND ----------

# MAGIC %md ## Summary (screenshot this table)

# COMMAND ----------

summary = spark.createDataFrame(results, "check string, status string, detail string, fallback string")
display(summary)

# COMMAND ----------

# MAGIC %md ## Clean up scratch objects

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "yes":
    if not SCHEMA.startswith("_platform_check"):
        print(f"Refusing to drop schema {SCHEMA!r}: only schemas starting with '_platform_check' are cleaned up.")
    else:
        spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{SCHEMA}` CASCADE")
        print(f"Dropped {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual checks (the notebook cannot do these for you)
# MAGIC 1. **Failure notification**: Jobs & Pipelines > create a one-task Job whose notebook raises an exception, add your e-mail under *Notifications > Failure*, run it, and see whether the e-mail arrives.
# MAGIC 2. **Power BI**: open Power BI Desktop > Get Data > *Databricks*; use the `server_hostname` and `http_path` printed in check 7, and a personal access token (Settings > Developer). Whether Free Edition lets you create a token or connect from outside is **not assumed**: if it fails, Gold is exported to CSV.
# MAGIC 3. **Screenshots** to save later in `docs/screenshots/`: the summary table above, and the manual-check outcomes.
