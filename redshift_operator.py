import redshift_connector
import awswrangler as wr


class RedshiftOperator:

    def __init__(self, host_name, user_name, password, database, staging_directory):
        self.conn = redshift_connector.connect(
            user=user_name,
            password=password,
            host=host_name,
            database=database
        )

        self.staging_directory = staging_directory

    def ingest_df(self, df, schema, table, mode) -> None:

        wr.redshift.copy(
            df=df,
            path=self.staging_directory,
            con=self.conn,
            table=table,
            schema=schema,
            mode=mode
        )

    def generate_report(self) -> None:

        sql_average = """SELECT avg(duration) as average_trip_time
        FROM public.activity
        WHERE extract(year from started_at) = 2022
        """

        sql_common_pair = """SELECT s1.station_name as start_station, s2.station_name as end_station, COUNT(*) as occurrences
        FROM public.activity a
        LEFT JOIN station s1
        ON a.start_station_id = s1.station_id
        LEFT JOIN station s2
        ON a.end_station_id = s2.station_id
        WHERE extract(year from a.started_at) >= 2022
        GROUP BY start_station, end_station
        ORDER BY COUNT(*) DESC
        """

        df_ave = wr.redshift.read_sql_query(
            sql=sql_average,
            con=self.conn
        )

        df_ave.to_csv('ave.csv', index=False)

        df_common_pair = wr.redshift.read_sql_query(
            sql=sql_common_pair,
            con=self.conn
        )

        df_common_pair.to_csv('common_pair.csv', index=False)

    def __del__(self):
        self.conn.close()



