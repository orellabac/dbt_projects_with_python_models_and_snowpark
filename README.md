## dbt-snowpark-service-now

Ingest ServiceNow project baseline data into Snowflake using a dbt Python model with Snowpark, then transform it with SQL models into a small financial mart.

### Intent
- Demonstrate calling the ServiceNow Table API from a dbt Python model using Snowpark with secure egress via a Snowflake External Access Integration (EAI).
- Show a typical dbt layering pattern: staging (Python), intermediate (SQL), marts (SQL aggregates).

### Model flow
1) `stg_servicenow_pm_project_baseline.py` (Python) calls the ServiceNow `pm_project_baseline` endpoint, returns a Snowpark DataFrame and materializes it as a table.
2) `int_servicenow_projects.sql` normalizes/casts numeric columns, deduplicates by project, and derives a size bucket.
3) `mart_portfolio_financials.sql` produces simple portfolio-level KPIs.

### Key files
- `dbt_project.yml`: Project and variables (`INSTANCE`, `USER`, `PASSWORD`).
- `profiles.yml`: Snowflake connection profile used by dbt.
- `setup.sql`: Creates an External Access Integration allowing `*.service-now.com`.
- `models/staging/stg_servicenow_pm_project_baseline.py`: Snowpark Python model invoking ServiceNow via `requests`.
- `models/config.yml`: Wires model-level configs to `vars`.
- `models/intermediate/int_servicenow_projects.sql`: SQL normalization/deduplication.
- `models/marts/mart_portfolio_financials.sql`: Final aggregated mart.

### Setup
1) In Snowflake, run `setup.sql` with a role that can create EAIs.
2) In `dbt_project.yml`, set:
   - `INSTANCE`: your ServiceNow instance base URL (e.g., `https://your-instance.service-now.com`)
   - `USER`: ServiceNow username
   - `PASSWORD`: ServiceNow password (prefer passing via `--vars` or environment)
3) Update `profiles.yml` with your Snowflake connection details.
4) Ensure an External Access Integration exists and is referenced by the Python model (the sample registers using an integration available in your account).



### Outputs
- `stg_servicenow_pm_project_baseline` (table)
- `int_servicenow_projects` (view/table depending on project config)
- `mart_portfolio_financials` (view/table depending on project config)
