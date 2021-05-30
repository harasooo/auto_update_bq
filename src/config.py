import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account


gcp_cred_notation: str = str(os.getenv("CREDENTIAL_NOTATION"))
gcp_cred_envs = [i for i in os.environ if "GCP_CREDENTIAL" in i]
gcp_cred_keys = [i[len(gcp_cred_notation):].lower() for i in gcp_cred_envs]
gcp_cred_values = [str(os.getenv(i)).replace("\\n", "\n")
                   for i in gcp_cred_envs]
cred_dict = dict(zip(gcp_cred_keys, gcp_cred_values))

with open(str(os.getenv("MY_KEY_PATH")), 'w') as f:
    json.dump(cred_dict, f, ensure_ascii=False, indent=0)


if os.getenv("ENV") == "local":
    json_acct_info = json.load(open(str(os.getenv("MY_KEY_PATH"))))
    credentials = service_account.Credentials.from_service_account_info(
        json_acct_info)
    CREDENTIALS = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])
    PROJECT_ID = os.getenv("PROJECT_ID")
    TABLE_ID = os.getenv("TABLE_ID")
    CLIENT = bigquery.Client(credentials=credentials, project=PROJECT_ID)
