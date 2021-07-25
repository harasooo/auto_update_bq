import os
import json
from typing import Any
from google.cloud import bigquery
from google.oauth2 import service_account


def global_schema(client: Any, project_id: str, table_id: str) -> Any:
    dataset = client.get_dataset(str(os.getenv("DATASET_ID")))
    tables = client.list_tables(dataset)
    if tables:
        for obj in tables:
            if str(obj.reference) == ".".join([project_id, table_id]):
                table = client.get_table(obj.reference)
                schema = table._properties.get("schema").get("fields")
    return schema


def client_credentials_from_envs() -> Any:
    gcp_cred_notation: str = str(os.getenv("CREDENTIAL_NOTATION"))
    gcp_cred_envs = [i for i in os.environ if "GCP_CREDENTIAL" in i]
    gcp_cred_keys = [i[len(gcp_cred_notation) :].lower() for i in gcp_cred_envs]
    gcp_cred_values = [str(os.getenv(i)).replace("\\n", "\n") for i in gcp_cred_envs]
    cred_dict = dict(zip(gcp_cred_keys, gcp_cred_values))

    with open(str(os.getenv("MY_KEY_PATH")), "w") as f:
        json.dump(cred_dict, f, ensure_ascii=False, indent=0)

    json_acct_info = json.load(open(str(os.getenv("MY_KEY_PATH"))))
    credentials = service_account.Credentials.from_service_account_info(json_acct_info)
    credentials = credentials.with_scopes(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )

    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return credentials, client


PROJECT_ID = str(os.getenv("PROJECT_ID"))
TABLE_ID = str(os.getenv("TABLE_ID"))
TIMEDELTA = int(os.getenv("TIMEDELTA"))
if os.getenv("ENV") == "GCP":
    CLIENT = bigquery.Client()
    CREDENTIALS = "default"
else:
    CREDENTIALS, CLIENT = client_credentials_from_envs()

SCHEMA = global_schema(CLIENT, PROJECT_ID, TABLE_ID)
