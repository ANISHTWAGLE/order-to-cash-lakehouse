# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Create (or update) the pipeline Job
# MAGIC Creates the Job **`o2c_lakehouse_pipeline`** with tasks
# MAGIC `bronze_ingestion → silver_transformations → gold_star_schema → dq_checks → table_maintenance`,
# MAGIC serverless compute, a failure e-mail and a daily schedule (created **paused**).
# MAGIC Running it again updates the same Job instead of creating a second one.
# MAGIC
# MAGIC The same Job is also defined as code in `databricks.yml` (Databricks Asset Bundle) for
# MAGIC deployment with the Databricks CLI.

# COMMAND ----------

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("notify_email", "", "Failure e-mail (blank = your user)")
dbutils.widgets.dropdown("run_now", "yes", ["yes", "no"], "Run the Job after creating it")

w = WorkspaceClient()
catalog = dbutils.widgets.get("catalog")
email = dbutils.widgets.get("notify_email") or spark.sql("SELECT current_user()").first()[0]
nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
JOB_NAME = "o2c_lakehouse_pipeline"

steps = [
    ("bronze_ingestion", "03_bronze_ingestion", None),
    ("silver_transformations", "04_silver_transformations", "bronze_ingestion"),
    ("gold_star_schema", "05_gold_star_schema", "silver_transformations"),
    ("dq_checks", "06_dq_and_reconciliation", "gold_star_schema"),
    ("table_maintenance", "07_table_maintenance", "dq_checks"),
]
tasks = [
    jobs.Task(
        task_key=key,
        depends_on=[jobs.TaskDependency(task_key=dep)] if dep else [],
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{nb_dir}/{notebook}",
            base_parameters={"catalog": catalog, "run_id": "{{job.run_id}}"},
        ),
        max_retries=0,
        timeout_seconds=3600,
    )
    for key, notebook, dep in steps
]
settings = dict(
    name=JOB_NAME,
    tasks=tasks,
    email_notifications=jobs.JobEmailNotifications(on_failure=[email]),
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC",
                               pause_status=jobs.PauseStatus.PAUSED),
    max_concurrent_runs=1,  # two overlapping runs would race on the same watermarks
    tags={"project": "order-to-cash-lakehouse"},
)

existing = [j for j in w.jobs.list(name=JOB_NAME)]
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id=job_id, new_settings=jobs.JobSettings(**settings))
    print(f"Updated job {job_id}")
else:
    job_id = w.jobs.create(**settings).job_id
    print(f"Created job {job_id}")
print(f"Failure e-mail: {email}")

# COMMAND ----------

if dbutils.widgets.get("run_now") == "yes":
    run = w.jobs.run_now(job_id=job_id)
    print(f"Started run {run.run_id}. Watch it under Jobs & Pipelines > {JOB_NAME} > Runs.")
