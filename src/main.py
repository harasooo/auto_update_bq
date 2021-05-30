from config import PROJECT_ID, TABLE_ID, CLIENT, CREDENTIALS
from scrape import get_exist_race_list, scrape
import warnings

warnings.simplefilter('ignore')


def main():
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    exist_race_list = get_exist_race_list(CLIENT)
    scrape(exist_race_list, TABLE_ID, PROJECT_ID, CREDENTIALS)


if __name__ == "__main__":
    main()
