import datetime
import pandas as pd
from pandas import DataFrame
from dateutil.relativedelta import relativedelta
from urllib.error import HTTPError
import sys


class Crawler:

    def __init__(self, base_url):
        self.base_url = self.trim_last_slash(base_url)

    def trim_last_slash(self, url: str) -> str:
        if url[-1] == '/':
            return url[:-1]
        return url

    def url_builder(self, year: str, month: str) -> str:
        url = self.base_url + "/" + year + "/" + month + ".csv"
        return url

    def crawl(self, start_date: datetime, end_date: datetime) -> tuple[DataFrame, DataFrame]:

        crawl_date = start_date
        full_df = None

        while not ((crawl_date.year > end_date.year) or
                   (crawl_date.year == end_date.year and crawl_date.month > end_date.month)):
            year = str(crawl_date.year)
            month = str(crawl_date.month).zfill(2)
            url = self.url_builder(year, month)

            try:
                if full_df is None:
                    full_df = pd.read_csv(url)
                else:
                    df = pd.read_csv(url)
                    full_df = pd.concat([df, full_df])
            except HTTPError as e:
                print(url + ": " + str(e))
                sys.exit(1)

            crawl_date = crawl_date + relativedelta(months=1)

        # TODO: Not only dropping null values but also validate each record
        full_df = full_df.dropna()

        full_df["started_at_date"] = pd.to_datetime(full_df.started_at).dt.date
        full_df["started_at"] = pd.to_datetime(full_df.started_at)
        full_df["ended_at"] = pd.to_datetime(full_df.ended_at)

        text_columns = ["start_station_name", "start_station_description", "end_station_name", "end_station_description"]

        full_df[text_columns] = full_df[text_columns].applymap(self.convert_norwegian)
        full_df = full_df[(start_date <= full_df.started_at_date) & (full_df.started_at_date <= end_date)]

        activity_columns = ["started_at", "ended_at", "duration", "start_station_id", "end_station_id"]
        start_station_columns = ["start_station_id", "start_station_name", "start_station_description",
                                 "start_station_latitude", "start_station_longitude"]
        end_station_columns = ["end_station_id", "end_station_name", "end_station_description",
                               "end_station_latitude", "end_station_longitude"]
        station_columns = ["station_id", "station_name", "station_description", "station_latitude", "station_longitude"]

        start_station_column_mapping = dict(zip(start_station_columns, station_columns))
        end_station_column_mapping = dict(zip(end_station_columns, station_columns))

        activity_df = full_df[activity_columns]
        start_station_df = full_df[start_station_columns].rename(start_station_column_mapping, axis=1)
        end_station_df = full_df[end_station_columns].rename(end_station_column_mapping, axis=1)
        station_df = pd.concat([start_station_df, end_station_df]).drop_duplicates(subset=['station_id'])

        return activity_df, station_df

    def convert_norwegian(self, s: str) -> str:
        return s.replace('æ', 'ae').replace('ø', 'oe').replace('å', 'aa')
