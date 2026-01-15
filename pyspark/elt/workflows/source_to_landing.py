"""ELT workflow: Source -> Landing

This script is the Dataproc Serverless entrypoint. It:
1) Queries INFORMATION_SCHEMA for table metadata
2) Builds Spark schema
3) Reads source data using provided query
4) Adds _extract_date_
5) Writes Parquet to GCS landing path

All tables use this single entrypoint; table-specific behavior is driven by args.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from aw.schema import get_source_columns_info, build_spark_schema


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--jdbc_url", required=True)
    p.add_argument("--jdbc_driver", required=True)
    p.add_argument("--jdbc_user", required=True)
    p.add_argument("--jdbc_password", required=True)

    p.add_argument("--jdbc_schema_or_database", required=True)
    p.add_argument("--table_name", required=True)

    # query built by Airflow builder
    p.add_argument("--query", required=True)

    # output
    p.add_argument("--output_path", required=True)
    p.add_argument("--output_format", default="parquet")
    p.add_argument("--output_compression", default="snappy")

    # mysql | postgres
    p.add_argument("--source_type", required=True, choices=["mysql", "postgres"])

    return p.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("source_to_landing").getOrCreate()

    # 1) get schema info
    columns_info = get_source_columns_info(
        spark=spark,
        jdbc_url=args.jdbc_url,
        jdbc_driver=args.jdbc_driver,
        user=args.jdbc_user,
        password=args.jdbc_password,
        schema=args.jdbc_schema_or_database,
        table=args.table_name,
    )

    # 2) build spark schema
    spark_schema = build_spark_schema(columns_info, args.source_type)

    # 3) read data using query
    df = (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("driver", args.jdbc_driver)
        .option("user", args.jdbc_user)
        .option("password", args.jdbc_password)
        .option("dbtable", f"({args.query}) AS t")
        .schema(spark_schema)
        .load()
    )

    # 4) add extract_date
    df = df.withColumn("_extract_date_", current_timestamp())

    # 5) write parquet to landing
    df.write.mode("overwrite").format(args.output_format).option(
        "compression", args.output_compression
    ).save(args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()
