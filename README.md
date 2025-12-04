## dbt projects with Python models and Snowpark (Snowflake)

This repository accompanies an article about leveraging dbt Python models with Snowpark in Snowflake. It contains two small, self-contained dbt projects that demonstrate fetching data from external APIs into Snowflake via Snowpark, then transforming it with standard SQL models.

### Projects
- **dbt-snowpark-google-sheets**: Reads tabular data from Google Sheets using a Snowpark UDF with a Google service account secret and an External Access Integration (EAI). Subsequent SQL models compute marketing KPIs.
- **dbt-snowpark-service-now**: Calls the ServiceNow Table API via a Snowpark stored procedure to ingest project baseline data, then applies SQL transformations and a simple mart.

### Repository layout
- `dbt-snowpark-google-sheets/`
  - `dbt_project.yml`, `profiles.yml`, `setup.sql`
  - `models/`
    - `staging/` (Python model pulling Google Sheets)
    - `intermediate/` (SQL transforms, casting/deriving KPIs)
    - `marts/` (final aggregates)
- `dbt-snowpark-service-now/`
  - `dbt_project.yml`, `profiles.yml`, `setup.sql`
  - `models/`
    - `staging/` (Python model calling ServiceNow API)
    - `intermediate/` (SQL transforms, normalization/bucketing)
    - `marts/` (final aggregates)

### What these examples show
- **Python models in dbt**: Python files under `models/` that return a Snowpark DataFrame become dbt models you can `ref()` from SQL.
- **Snowpark + external APIs**: Secure egress via Snowflake External Access Integrations and, where needed, Snowflake Secrets for credentials.
- **Typical dbt layering**: `staging` (ingest/shape), `intermediate` (business logic), `marts` (consumable outputs).

### Prerequisites
- Snowflake account with Snowpark enabled and a role that can create network rules, external access integrations, and (for Google Sheets) secrets.
- dbt (Core or via Snowflake CLI) with the Snowflake adapter.
- Ability to run the provided `setup.sql` scripts in Snowflake.

### Quick start (common steps)
1) Pick a project directory and review its `dbt_project.yml` and `profiles.yml`.
2) In Snowflake, run that project's `setup.sql` to create required EAI (and Secret for Google Sheets).
3) Update variables in the project's `dbt_project.yml` (e.g., `SECRET_NAME`, `SPREADSHEET_ID`, `INSTANCE`, etc.).
4) Update `profiles.yml` with your Snowflake connection details.


Notes:
- Some secrets/variables (e.g., passwords) can be provided via `--vars` or environment variables instead of committing them to files.
- Ensure the External Access Integration name used in the Python model matches one available in your account.
