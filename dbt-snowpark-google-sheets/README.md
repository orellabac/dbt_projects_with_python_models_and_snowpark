## dbt-snowpark-google-sheets

Ingest tabular data from Google Sheets into Snowflake using a dbt Python model with Snowpark, then transform it with SQL models into a small KPI mart.

### Intent
- Demonstrate dbt Python models that call external APIs (Google Sheets) via a Snowflake External Access Integration and a Snowflake Secret for service-account credentials.
- Show a typical dbt layering pattern: staging (Python), intermediate (SQL), marts (SQL aggregates).

### Model flow
1) `stg_google_sheets_campaigns.py` (Python) reads a range from a Google Sheet through a Snowpark UDF, returning a DataFrame materialized as a table.
2) `int_google_sheets_campaigns.sql` casts and derives KPI metrics (CTR, CPC, CPA, ROAS, spend buckets).
3) `mart_campaign_kpis.sql` aggregates KPIs by `channel` and `date`.

### Key files
- `dbt_project.yml`: Project and variables (`SECRET_NAME`, `SPREADSHEET_ID`, `RANGE_NAME`, `EAI_NAME`).
- `profiles.yml`: Snowflake connection profile used by dbt.
- `setup.sql`: Creates a Snowflake Secret for the Google service account and an External Access Integration allowing `*.googleapis.com`.
- `models/staging/stg_google_sheets_campaigns.py`: Snowpark Python model that calls the Google Sheets API via a UDF.
- `models/staging/config.yml`: Wires model-level configs to `vars`.
- `models/intermediate/int_google_sheets_campaigns.sql`: SQL transforms and KPI derivations.
- `models/marts/mart_campaign_kpis.sql`: Final aggregated mart.

### Setup
1) In Snowflake, run `setup.sql` (with a role that can create secrets and EAIs). Replace the example secret JSON with your Google service account JSON. Share the target sheet with the service account email.
2) In `dbt_project.yml`, set:
   - `SECRET_NAME`: the name of the Snowflake Secret that holds your service account JSON
   - `SPREADSHEET_ID`: the Google Sheet ID from the URL
   - `RANGE_NAME`: e.g., `Sheet1!A1:G1000` or simply `Sheet1`
   - `EAI_NAME`: the External Access Integration name created by `setup.sql`
3) Update `profiles.yml` with your Snowflake connection details.

### Run
Using dbt-core:
```bash
pip install dbt-snowflake snowflake-snowpark-python
DBT_PROFILES_DIR=. dbt debug
DBT_PROFILES_DIR=. dbt run
```

Using Snowflake CLI:
```bash
snow dbt debug dbt_snowpark_google_sheets
snow dbt run dbt_snowpark_google_sheets
```

### Outputs
- `stg_google_sheets_campaigns` (table)
- `int_google_sheets_campaigns` (view/table depending on project config)
- `mart_campaign_kpis` (view/table depending on project config)
