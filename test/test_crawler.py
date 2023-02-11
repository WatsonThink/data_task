import unittest
from crawler import Crawler
import datetime
import pandas as pd


class TestCrawler(unittest.TestCase):

    def setUp(self) -> None:
        BASE_URL = 'https://data.urbansharing.com/oslobysykkel.no/trips/v1'
        self.crawler = Crawler(BASE_URL)

    def test_incremental_crawl(self):
        date = datetime.date.today()

        df_activity, _ = self.crawler.crawl(start_date=date, end_date=date)
        df_activity["started_at_date"] = pd.to_datetime(df_activity.started_at).dt.date
        df_activity_today = df_activity[df_activity.started_at_date == date]
        self.assertEqual(df_activity.shape[0], df_activity_today.shape[0])

    def test_convert_norwegian(self):

        s = "æøå"
        s_converted = self.crawler.convert_norwegian(s)
        self.assertEqual(s_converted, "aeoeaa")



