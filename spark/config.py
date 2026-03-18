"""Spark session builder and Snowflake connector configuration."""

import os

from pyspark.sql import SparkSession

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
# SPARK_PACKAGES = "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,net.snowflake:snowflake-jdbc:3.18.0"
SPARK_PACKAGES = "net.snowflake:spark-snowflake_2.12:3.1.6,net.snowflake:snowflake-jdbc:3.18.0"


def create_spark_session(app_name: str = "fulfillment-features") -> SparkSession:
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def snowflake_options(schema: str = "MARTS") -> dict:
    return {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "FULFILLMENT_DB"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "FULFILLMENT_WH"),
        "sfSchema": schema,
    }


def read_snowflake_table(spark: SparkSession, table: str, schema: str = "MARTS", query: str = None):
    opts = snowflake_options(schema)

    if query:
        opts["query"] = query
        return spark.read.format(SNOWFLAKE_SOURCE).options(**opts).load()

    opts["dbtable"] = table
    return spark.read.format(SNOWFLAKE_SOURCE).options(**opts).load()


def write_snowflake_table(df, table: str, schema: str = "STAGING", mode: str = "overwrite"):
    opts = snowflake_options(schema)
    opts["dbtable"] = table

    df.write.format(SNOWFLAKE_SOURCE).options(**opts).mode(mode).save()
