
## Architecture

I use Redshift as a database to store historical data from Oslo City Bike API.
Two tables are defined, Activity table as a fact table and Station table as a dimension table.

Crawler retrieves historical data from API and narrows down the range of date using start_date and end_date passed
as arguments of the crawl function.
The crawl function generate two dataframes activity dataframe for the activity table and station dataframe for the
station table.
RedshiftOperator's ingest_df function takes these dataframes as arguments and write into each table.
When writing station dataframe into the table, I use upsert mode to avoid writing duplicate records and prioritize
the latest station information if there are some update of station name or description.
RedshiftOperator has also generate_report function to report the following insights as csv format.

- What is the average trip time for all trips in the year 2022?
- What are the most common start and end station pairs, sorted from the most common to
least common, from all trips taken from 2022 to the most recent data?

## Part2 Questions

1. How would you retrieve the data that is updated daily and incorporate it into your dataset?

The first time of ingesting the historical data from API, we can pass start_date as 20xx-xx-xx and end_date as today.
The following incremental update of the historical data, we can just pass start_date as today and end_date as also today.
RedshiftOperator's ingest_df function will append this incremental data into activity table.

a. How would you schedule it?

We can use Apache Airflow to schedule and orchestrate multiple tasks.
For example, we can define three tasks in a dag file with daily scheduling configuration.

task1: incrementally retrieve the data from API and store it on object storage like S3

task2: Copy data on S3 into tables on Redshift

task3: Generate reports

2. Imagine that you have to maintain a list of the most common start-end station pairs:

a. How would you design the data model?

We can define materialized view which calculates a list of the most common start-end station pairs.

b. How would you keep it up to date as new data comes in?

We can update the materialized view as soon as ingesting a new data into table on task2 described in the question 1.

3. Assume there is an update to the bike infrastructure in Oslo some months in the future and
some of the existing bike stations are removed. How would you modify your model to take
into account historical stations that are no longer used?

The station table is independent of the activity table.
We can add a new column like "is_active" on the station table to see whether a station still exists or not.
