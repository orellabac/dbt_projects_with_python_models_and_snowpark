CREATE OR REPLACE NETWORK RULE service_now_network_rule
  TYPE = 'HOST_PORT'
  MODE= 'EGRESS'
  VALUE_LIST = ('*.service-now.com:443','*.service-now.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION service_now_apis_access_integration
  ALLOWED_NETWORK_RULES = (service_now_network_rule)
  ENABLED = true;
