from config import PROJECT_ID, TABLE_ID, CLIENT, CREDENTIALS, SCHEMA
from scrape import get_exist_race_list, get_race_list, write_bq
import warnings

warnings.simplefilter('ignore')


def main():
    exist_race_list = get_exist_race_list(CLIENT)
    race_list = get_race_list()
    write_bq(race_list, exist_race_list, TABLE_ID,
             PROJECT_ID, CREDENTIALS, SCHEMA)
