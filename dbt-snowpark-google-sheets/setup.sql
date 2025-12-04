-- I assume you are creating this elements in the same database where 
-- your dbt projects are

CREATE OR REPLACE SECRET GSHEETS_SECRETS
  TYPE = GENERIC_STRING
  SECRET_STRING = 
  $$
{
  "type": "service_account",
  "project_id": "<your-project-id>",
  "private_key_id": "9999999999999999999999",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE.....==\n-----END PRIVATE KEY-----\n",
  "client_email": "gsheetsa@<your-project-id>.iam.gserviceaccount.com",
  "client_id": "115020149802079506987",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gsheetsa%40<your-project-id>.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
$$;

CREATE OR REPLACE NETWORK RULE google_apis_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('*.googleapis.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION google_apis_access_integration
  ALLOWED_NETWORK_RULES = (google_apis_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (GSHEETS_SECRETS)
  ENABLED = true;