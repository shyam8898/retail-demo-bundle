"""Starts the prod medallion pipeline and waits for the update to finish.

Run by CI after deploy pass 2, so a single push results in a fully built
prod environment (job + catalog + pipeline + bronze/silver/gold data).

Auth comes from DATABRICKS_HOST / DATABRICKS_TOKEN env vars.
"""
import os
import sys
import time

from databricks.sdk import WorkspaceClient

PIPELINE_NAME = "Retail Medallion Pipeline"

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

pid = None
for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"):
    if p.name == PIPELINE_NAME:
        pid = p.pipeline_id
        break
if pid is None:
    print(f"ERROR: pipeline '{PIPELINE_NAME}' not found")
    sys.exit(1)

print(f"Starting pipeline {PIPELINE_NAME} (id {pid})...")
try:
    update = w.pipelines.start_update(pipeline_id=pid)
    update_id = update.update_id
except Exception as e:  # already running: fall through and watch the active update
    print(f"start_update() said: {e} — continuing by watching the latest update")
    update_id = None

print("Waiting for the pipeline update to finish (this builds bronze/silver/gold)...")
for _ in range(120):  # up to ~20 minutes
    time.sleep(10)
    if update_id is None:
        update_id = w.pipelines.list_updates(pipeline_id=pid).updates[0].update_id
    state = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update.state
    state = getattr(state, "value", state)  # enum -> string, e.g. UpdateInfoState.COMPLETED
    if state == "COMPLETED":
        print("Pipeline update COMPLETED — prod is fully built.")
        sys.exit(0)
    if state in ("FAILED", "CANCELED"):
        print(f"ERROR: pipeline update ended with state {state}")
        sys.exit(1)
    print(f"  update state: {state}")

print("ERROR: timed out waiting for the pipeline update")
sys.exit(1)
