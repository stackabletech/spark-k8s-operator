"""
Creates a report with three indicators out of the NY TLC data set.

See: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

It accepts two command line arguments:
--input     Path to the input data source. Can be a local path, a S3 object
            or whatever else Spark supports. Additional dependencies might
            need to be submitted along with the job.
--output    Path to write the report as a CSV file.
"""
import argparse

from argparse import Namespace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import dayofweek


def check_args() -> Namespace:
    """Parse the given CLI arguments"""
    parser = argparse.ArgumentParser(description="NY taxi trip report")
    parser.add_argument("--input", "-i", required=True, help="Input path for dataset")
    parser.add_argument(
        "--output", "-o", required=False, help="Output path for the report."
    )
    return parser.parse_args()


def build_report(spark: SparkSession, args: Namespace) -> DataFrame:
    """Compute the total number of passangers plus the average fare and distance per day of week"""

    input_df = spark.read.options(header=True, inferSchema=True).csv(args.input)

    return (
        input_df.select(
            dayofweek(input_df["tpep_pickup_datetime"]).alias("day_of_week"),
            input_df["passenger_count"],
            input_df["trip_distance"],
            input_df["total_amount"],
        )
        .groupby("day_of_week")
        .agg({"passenger_count": "sum", "trip_distance": "avg", "total_amount": "avg"})
        .withColumnRenamed("avg(total_amount)", "avg_amount")
        .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")
        .withColumnRenamed("sum(passenger_count)", "total_passengers")
        .orderBy("day_of_week")
    )


if __name__ == "__main__":
    args = check_args()

    spark = SparkSession.builder.appName("NY TLC Report").getOrCreate()

    try:
        report = build_report(spark, args)
        report.show()
        if args.output:
            report.coalesce(1).write.mode("overwrite").options(header=True).csv(
                args.output
            )
    finally:
        spark.stop()
