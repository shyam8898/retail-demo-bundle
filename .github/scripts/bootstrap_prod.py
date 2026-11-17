"""Bootstraps the prod environment by running the setup job once.

The setup job (seed_landing_data notebook) creates the Unity Catalog
objects (catalog, schemas, landing volume) with SQL DDL and seeds the demo
CSV files. The pipeline cannot be created before the catalog exists, which
is why the workflow runs this between the two deploy passes.

Idempotent: re-running it is harmless (all DDL is IF NOT EXISTS).
Auth comes from DATABRICKS_HOST / DATABRICKS_TOKEN env vars.
"""
import os
import sys

from databricks.sdk import WorkspaceClient

JOB_NAME = "Retail Demo - Seed Landing Volume"

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

job_id = next((j.job_id for j in w.jobs.list(name=JOB_NAME)), None)
if job_id is None:
    print(f"ERROR: job '{JOB_NAME}' not found — deploy pass 1 must run first")
    sys.exit(1)

print(f"Running setup job {JOB_NAME} (id {job_id})...")
run = w.jobs.run_now(job_id).result()
state = run.state.result_state
if state != "SUCCESS":
    print(f"ERROR: setup job finished with state {state}")
    sys.exit(1)

print("Catalog, schemas, volume and landing data are ready.")
