# Retail Medallion — DAB (dev -> prod) demo

> **Deployment is fully automated via GitHub Actions**: every push to `main`
> deploys the complete stack to prod (setup job -> catalog + seed data ->
> pipeline -> build bronze/silver/gold). No manual steps anywhere.

This bundle moves the retail medallion pipeline from the **dev** workspace to
**prod** (`https://dbc-7a752a54-93ab.cloud.databricks.com`) and keeps them in
sync: any code change is made once here and redeployed to both targets.

## What the bundle contains

| Path | What it is |
| --- | --- |
| `databricks.yml` | Bundle name, variables, dev + prod targets |
| `resources/retail_demo_uc.yml` | Note on Unity Catalog objects (created by the setup job, see below) |
| `resources/retail_medallion_pipeline.pipeline.yml` | The SDP pipeline (bronze/silver/gold + DQ) |
| `resources/seed_landing_data.job.yml` | One-time setup job: creates the UC objects and seeds the landing volume |
| `src/transformations/**` | Pipeline source code (one dataset per file, by layer) |
| `src/notebooks/seed_landing_data` | The setup/seeding notebook run by the job |

## First-time setup in a new environment

1. `databricks bundle validate -t <target>` — check the config resolves
2. `databricks bundle deploy -t <target>` — creates/updates the pipeline and the setup job
3. Run the **Retail Demo - Seed Landing Volume** job once — it creates the catalog
   `retail_demo`, the bronze/silver/gold/raw schemas, the landing volume, and
   populates the CSVs. (UC objects are created by the job, not by bundle
   resources, because this account uses Default Storage and API catalog
   creation needs an explicit managed location — SQL `CREATE CATALOG` works.)
4. Start the pipeline — bronze ingests, silver dedups/SCDs, gold MVs refresh

## Day-2 workflow: a code change goes everywhere

1. Edit the code under `src/transformations/` (e.g. change a gold aggregation)
2. `databricks bundle deploy -t dev` — dev pipeline picks up the change; run it and verify
3. `databricks bundle deploy -t prod` — the same change lands in prod

Because both pipelines read their source from the bundle's `src/` copies,
redeploying is all it takes to propagate a change.

## Authenticating to prod

The CLI is already logged in to dev. To deploy to prod, give it prod
credentials for that one command:

```
DATABRICKS_HOST=https://dbc-7a752a54-93ab.cloud.databricks.com \
DATABRICKS_TOKEN=<prod PAT> \
databricks bundle deploy -t prod
```

(Or add a `prod` auth profile to `.databrickscfg` and set
`workspace.auth_profile: prod` on the prod target in `databricks.yml`.)

## Notes for the junior session

* `mode: development` (dev target) prefixes resource names with `[DEV <user>]`
  so dev deploys can never touch prod-like shared resources; `mode: production`
  (prod target) requires a fixed `run_as` identity.
* The `engine: direct` setting enables catalog resources (Unity Catalog
  objects) in the bundle.
* Same catalog name `retail_demo` is used in both workspaces — they are separate
  metastore objects, so there is no collision.
* Demo scenarios baked into the data: SCD2 (C003 segment change), dedup
  (O1001 re-drop), schema evolution (`delivery_notes` in the 2nd orders
  batch), DQ quarantine (OI27/OI28 bad quantities, PM17 negative amount,
  PM18 invalid method).
