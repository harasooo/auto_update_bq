from bs4 import BeautifulSoup
from typing import List, Any, Dict
import re
import requests
import pandas as pd
import datetime
from datetime import timedelta


def get_exist_race_list(client, table):

    query_text = f"""
  select
  DISTINCT(race_id)
  FROM {table}
  """

    exist_race_list = client.query(query_text).to_dataframe()[
        "race_id"].to_list()
    return exist_race_list


def get_race_list() -> List[str]:
    end_date = datetime.date.today()
    start_date = end_date - timedelta(days=30)
    race_list = []
    while start_date < end_date:
        str_date = str(start_date).replace('-', '')
        index_url = f"https://db.netkeiba.com/race/list/{str_date}/"
        index_list = BeautifulSoup(requests.get(
            index_url).content, 'html').select('a[href*="/race/20"]')
        for index in index_list:
            race_list.append(index.get('href'))
        start_date = start_date + datetime.timedelta(days=1)
    return race_list


def get_table_list(race_list: List[str],
                   exist_race_list: List[str]) -> List[pd.DataFrame]:

    using_cols = ['race_id', '着順', '枠番', '馬番', '馬名', '性齢', '斤量', '騎手', 'タイム', '着差', '通過',
                  '上り', '単勝', '人気', '馬体重', '調教師', '馬主',
                  '賞金(万円)']

    col_name = ["race_id", "rank", "post_position", "horse_number", "horse_name", "age",
                "impost", "jockey", "time", "margin", "quantile_rank", "halon_time",
                "win_rate", "popular_rank", "weight", "Trainer", "owner", "prize",
                "race_name", "distance", "weather", "turf_state", "start_time",
                "race_round", "date", "race_track"]

    df_list = []
    for race_id in race_list:
        if int(re.sub(r'\D', '', race_id)) not in exist_race_list:
            race_url = f"https://db.netkeiba.com{race_id}"
            race_page_list = BeautifulSoup(requests.get(
                race_url).content, 'html')
            table_list = race_page_list.find_all("table")
            if len(table_list) > 0:
                df = pd.read_html(str(table_list[0]).replace(
                    "</diary_snap_cut>", "").replace("<diary_snap_cut>", ""))[0]
                df["race_id"] = re.sub(r'\D', '', race_id)
                df = df[using_cols]
                df["race_name"] = race_page_list.find_all("h1")[
                    1].get_text()
                df["distance"] = race_page_list.find_all(
                    "diary_snap_cut")[0].get_text().split('\n')[1].split("\xa0/\xa0")[0]
                df["weather"] = race_page_list.find_all(
                    "diary_snap_cut")[0].get_text().split('\n')[1].split("\xa0/\xa0")[1]
                df["turf_state"] = race_page_list.find_all(
                    "diary_snap_cut")[0].get_text().split('\n')[1].split("\xa0/\xa0")[2]
                df["start_time"] = race_page_list.find_all(
                    "diary_snap_cut")[0].get_text().split('\n')[1].split("\xa0/\xa0")[3]
                df["race_round"] = race_page_list.find_all("dl", class_="racedata")[
                    0].get_text().split('\n')[2]
                df["date"] = str(re.sub(r'\D', '', race_page_list.find_all(
                    "li", class_="result_link")[0].get_text()))
                df["race_track"] = race_page_list.find_all(
                    "a", class_="active")[0].get_text()
                df.columns = col_name
                df_list.append(df)
    return df_list


def write_bq(df_list: List[pd.DataFrame], dataset_id: str,
             project_id: str, credentials: Any, schema: List[Dict[str, str]]):

    if len(df_list) > 0:
        concat_race_df = pd.concat(df_list).astype(
            {'race_id': 'int64', 'impost': 'float'})
        if credentials == "default":
            concat_race_df.to_gbq(destination_table=dataset_id, project_id=project_id,
                                  if_exists='append',
                                  table_schema=schema)
        else:
            concat_race_df.to_gbq(destination_table=dataset_id, project_id=project_id,
                                  if_exists='append',
                                  table_schema=schema, credentials=credentials)
