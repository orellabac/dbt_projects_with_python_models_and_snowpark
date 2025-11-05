import pandas as pd
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark import types as T
import requests
from requests.auth import HTTPBasicAuth
import json



def model(dbt, session: Session):
    """
    This dbt Python model returns a fixed dataset.
    """
    # 1. Define the materialization for your model
    dbt.config(
        materialized="table"
    )

    # Replace these with your instance and credentials
    INSTANCE = dbt.config.get("INSTANCE")
    USER     = dbt.config.get("USER")
    PASSWORD = dbt.config.get("PASSWORD")

    def read_servicenow_data(session:Session) -> DataFrame:
        # Base URL for the ServiceNow Table API
        url = f"{INSTANCE}/api/now/table/pm_project_baseline"

        params = {
        "sysparm_display_value": "true",  # show readable values instead of sys_id
        "sysparm_limit": "5",             # just fetch a few to test
        "sysparm_fields": ",".join([
            "baseline_name",
            "sys_created_on",
            "pm_project.number",
            "pm_project.portfolio",
            "pm_project.program",
            "pm_project.cost",
            "pm_project.capex_cost",
            "pm_project.opex_cost",
            "pm_project.benefits",
            "pm_project.value",
            "pm_project.roi",
            "pm_project.discount_rate",
            "pm_project.npv_value",
            "pm_project.irr_value",
            "pm_project.resource_planned_cost",
            "pm_project.resource_allocated_cost",
            "pm_project.budget_cost",
            "pm_project.forecast_cost",
            "pm_project.estimate_to_completion",
            "pm_project.sys_id"
        ])
        }
        # Perform the GET request
        response = requests.get(
            url,
            params=params,
            auth=HTTPBasicAuth(USER, PASSWORD),
            headers={"Accept": "application/json"},
            timeout=30
        )
        if response.status_code == 200:
            results = response.json().get("result",[])
            df = pd.DataFrame(results)
            # Uppercase all column names and replace dots with underscores
            df.columns = df.columns.str.upper().str.replace(".", "_")
            return session.create_dataframe(df)
        else:
            raise Exception(f"❌ Error {response.status_code}: {response.text}")

    sproc_ref = session.sproc.register(
            external_access_integrations=["ALLOW_ALL_EAI"],  # List of external access integrations
            func=read_servicenow_data,                                 # The Python function to register
            packages=[],                               # List of required external packages
            is_permanent=False,                              # Set to True to make it permanent
            replace=True,                                    # Allows overwriting an existing procedure
            return_type=T.StructType(),         
            input_types=[]       # Input types of the Python function
        )


    result_df = sproc_ref()
    return result_df
    
