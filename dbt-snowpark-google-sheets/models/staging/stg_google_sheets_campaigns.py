import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark import types as T
import json

def model(dbt, session: Session):
    """
    Reads data from a Google Sheet and returns it as a Snowpark DataFrame.
    """
    dbt.config(materialized="table")

    PACKAGES = ['snowflake-snowpark-python','google-auth','google-auth-oauthlib','google-api-python-client']
    SECRET_NAME    = dbt.config.get("SECRET_NAME")
    SPREADSHEET_ID = dbt.config.get("SPREADSHEET_ID")
    RANGE_NAME     = dbt.config.get("RANGE_NAME")
    EAI_NAME       = dbt.config.get("EAI_NAME")
    if SECRET_NAME is None:
        raise ValueError("SECRET_NAME is not set")
    if SPREADSHEET_ID is None:
        raise ValueError("SPREADSHEET_ID is not set")
    if RANGE_NAME is None:
        raise ValueError("RANGE_NAME is not set")
    def get_google_sheet_data(spreadsheet_id, range_name):
        import json
        from snowflake.snowpark.secrets import get_generic_secret_string
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        """
        Reads data from a Google Sheet and returns it as JSON
        Args:
            spreadsheet_id: The ID of the spreadsheet (from the URL)
            range_name: The A1 notation of the range to retrieve (e.g., 'Sheet1!A1:D10')
        Returns:
            Dictionary with the sheet data in JSON format, or None if error occurs
        """
        try:
            secret_name = "service_account_info"
            json_string = get_generic_secret_string(secret_name)
            service_account_info = json.loads(json_string)
            creds = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES)    
            service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
            sheet = service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            values = result.get('values', [])
            # Convert to JSON format
            json_result = {
                'spreadsheet_id': spreadsheet_id,
                'range': range_name,
                'data': values,
                'row_count': len(values),
                'success': True
            }
            return json_result
            
        except HttpError as err:
            print(f'An error occurred: {err}')
            return {
                'spreadsheet_id': spreadsheet_id,
                'range': range_name,
                'error': str(err),
                'success': False
            }
    gsheets_udf = session.udf.register(get_google_sheet_data,
    name="gsheets_udf",
    is_permanent=False,
    return_type=T.VariantType(),
    input_types=[T.StringType(), T.StringType()],
    external_access_integrations=[EAI_NAME],
    secrets = {"service_account_info": SECRET_NAME},
    packages=PACKAGES)
    spreadsheet_data = session.sql(f"select gsheets_udf('{SPREADSHEET_ID}','{RANGE_NAME}')").first()[0] 
    data = json.loads(spreadsheet_data)
    return session.create_dataframe(data['data'][1:],schema=T.StructType([
        T.StructField("date",    T.StringType()),     # ISO date string from the sheet
        T.StructField("channel", T.StringType()),     # e.g., SEO, Paid Social, Email
        T.StructField("spend",   T.IntegerType()),      # numeric
        T.StructField("clicks",  T.IntegerType()),    # integer
        T.StructField("impressions", T.IntegerType()),# integer
        T.StructField("conversions", T.IntegerType()),# integer
        T.StructField("revenue", T.IntegerType())]))    # numeric