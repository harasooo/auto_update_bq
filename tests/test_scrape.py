import os
import re
import json
from src.scrape import get_table_list, get_race_list, get_exist_race_list, write_bq
from src.config import CLIENT, SCHEMA, PROJECT_ID, CREDENTIALS
import datetime
import time

TEST_TABLE_ID = str(os.getenv("TEST_TABLE_ID"))
YEAR = int(os.getenv("TEST_YEAR") or 0)
MONTH = int(os.getenv("TEST_MONTH") or 0)
START_DATE = int(os.getenv("TEST_START_DATE") or 0)
END_DATE = int(os.getenv("TEST_END_DATE") or 0)
TEST_START_DATE = datetime.date(YEAR, MONTH, START_DATE)
TEST_END_DATE = datetime.date(YEAR, MONTH, END_DATE)
TEST_DELETE_DATE = os.getenv("TEST_DELETE_DATE")


def get_wirted_race_id(client, table_id, test_delete_date):

    query_text = f"""
    select distinct race_id
    FROM {table_id}
    where date > {test_delete_date}
    """
    query_job = client.query(query_text).to_dataframe()[
        "race_id"].astype(str).to_list()
    return query_job


def delete_bq_race_id(client, table_id, test_delete_date):

    query_text = f"""
    delete
    FROM {table_id}
    where date > {test_delete_date}
    """
    client.query(query_text)


def test_race_list():

    delete_bq_race_id(CLIENT, TEST_TABLE_ID, TEST_DELETE_DATE)
    time.sleep(30)

    exist_race_id = get_exist_race_list(CLIENT, TEST_TABLE_ID)
    test_exist_race_dic = json.load(
        open("tests/data/test_exist_race_id.json", 'r')).get("results")
    test_exist_race_list = [str(i.get("race_id")) for i in test_exist_race_dic]
    exist_diff = set(exist_race_id) ^ set(test_exist_race_list)
    assert not exist_diff

    test_race_id = get_race_list("test", TEST_START_DATE, TEST_END_DATE)
    test_race_set = set([re.sub(r'\D', '', i) for i in test_race_id])
    test_get_race_dic = json.load(
        open("tests/data/test_get_race_id.json", 'r')).get("results")
    test_get_race_list = [str(i.get("race_id")) for i in test_get_race_dic]
    get_diff = set(test_get_race_list) ^ test_race_set
    assert not get_diff

    test_df = get_table_list(test_race_id, exist_race_id)
    get_tables_id = test_df["race_id"].astype(str).to_list()
    table_diff = set(get_tables_id) ^ (
        test_race_set - set(test_exist_race_list))
    assert not table_diff

    write_bq(test_df, TEST_TABLE_ID, PROJECT_ID, CREDENTIALS, SCHEMA)

    id_from_bq = get_wirted_race_id(CLIENT, TEST_TABLE_ID, TEST_DELETE_DATE)
    time.sleep(30)
    bq_diff = set(id_from_bq) ^ (
        test_race_set - set(test_exist_race_list))
    assert not bq_diff

    delete_bq_race_id(CLIENT, TEST_TABLE_ID, TEST_DELETE_DATE)
