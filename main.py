from crawler import Crawler
from redshift_operator import RedshiftOperator
import datetime
import boto3
from botocore.exceptions import ClientError
import json


def get_secret():

    secret_name = "test/redshift"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    return json.loads(secret)


def main():

    BASE_URL = 'https://data.urbansharing.com/oslobysykkel.no/trips/v1'
    START_DATE = datetime.date(2022, 1, 1)
    END_DATE = datetime.date.today()
    DATABASE = 'dev'

    crawler = Crawler(BASE_URL)
    activity_df, station_df = crawler.crawl(START_DATE, END_DATE)

    secret = get_secret()

    r_operator = RedshiftOperator(
        host_name=secret['hostname'],
        user_name=secret['username'],
        password=secret['password'],
        staging_directory=secret['staging_directory'],
        database=DATABASE
    )

    r_operator.ingest_df(df=activity_df, schema='public', table='activity', mode='append')
    r_operator.ingest_df(df=station_df, schema='public', table='station', mode='upsert')

    r_operator.generate_report()


if __name__ == "__main__":
    main()