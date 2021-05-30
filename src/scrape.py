from bs4 import BeautifulSoup
from typing import List, Any
import re
import requests
import pandas as pd
import datetime
from datetime import timedelta


def get_exist_race_list(client):

    query_text = """
  select
  DISTINCT(race_id)
  FROM test.test
  WHERE
  cast(
      left(
          cast(
              race_id as string
              ),
              8
          ) AS INT64
      ) >
  cast(
      replace(
          CAST(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 5 DAY) AS STRING),
          "-",
          "")
      as INT64
  )
  """

    exist_race_list = client.query(query_text).to_dataframe()[
        "race_id"].to_list()
    return exist_race_list


def scrape(exist_race_list: List[str], dataset_id: str,
           project_id: str, credentials: Any):
    using_cols = ['race_id', '着順', '枠番', '馬番', '馬名', '性齢', '斤量', '騎手', 'タイム', '着差', '通過',
                  '上り', '単勝', '人気', '馬体重', '調教師', '馬主',
                  '賞金(万円)']
    end_date = datetime.date.today()
    start_date = end_date - timedelta(days=10)
    race_list = []
    while start_date < end_date:
        str_date = str(start_date).replace('-', '')
        index_url = f"https://db.netkeiba.com/race/list/{str_date}/"
        index_list = BeautifulSoup(requests.get(
            index_url).content, 'html').select('a[href*="/race/20"]')
        for index in index_list:
            race_list.append(index.get('href'))
        start_date = start_date + datetime.timedelta(days=1)

    df_list = []
    odds_df_list = []
    for race_id in race_list:
        if int(re.sub(r'\D', '', race_id)) not in exist_race_list:
            race_url = f"https://db.netkeiba.com{race_id}"
            index_list = BeautifulSoup(requests.get(
                race_url).content, 'html').find_all("table")
            if len(index_list) > 0:
                df = pd.read_html(str(index_list[0]).replace(
                    "</diary_snap_cut>", "").replace("<diary_snap_cut>", ""))[0]
                df["race_id"] = re.sub(r'\D', '', race_id)
                df = df[using_cols]
                df.columns = ["race_id", "rank", "post_position", "horse_number",
                              "horse_name", "age", "impost", "jockey", "time",
                              "margin", "quantile_rank", "halon_time", "win_rate",
                              "popular_rank", "weight", "Trainer", "owner", "prize"]
                df_list.append(df)

                odds_df_1 = pd.read_html(str(index_list[1]))[0]
                odds_df_2 = pd.read_html(str(index_list[2]))[0]
                odds_df = pd.concat([odds_df_1, odds_df_2])
                odds_df["race_id"] = re.sub(r'\D', '', race_id)
                odds_df.columns = ["kind", "horse_number",
                                   "rate", "popular_rank", "race_id"]
                odds_df_list.append(odds_df)
    if len(df_list) > 0:
        concat_race_df = pd.concat(df_list).astype(
            {'race_id': 'int64', 'impost': 'float'})
        # concat_odds_df = pd.concat(odds_df_list)
        concat_race_df.to_gbq(destination_table=dataset_id, project_id=project_id,
                              if_exists='append', credentials=credentials)
