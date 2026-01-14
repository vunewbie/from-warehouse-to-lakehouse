"""
PySpark schema utilities for ELT pipeline.
"""

import re
from pyspark.sql.types import (
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
    BinaryType,
    StructType,
    StructField,
)
from pyspark.sql import SparkSession

# Type mapping dictionaries
MYSQL_TO_SPARK_DATA_TYPE = {
    "tinyint": "IntegerType",
    "smallint": "IntegerType",
    "mediumint": "IntegerType",
    "int": "IntegerType",
    "integer": "IntegerType",
    "bigint": "LongType",
    "float": "DoubleType",
    "double": "DoubleType",
    "decimal": "DecimalType",
    "numeric": "DecimalType",
    "char": "StringType",
    "varchar": "StringType",
    "text": "StringType",
    "longtext": "StringType",
    "mediumtext": "StringType",
    "tinytext": "StringType",
    "date": "DateType",
    "datetime": "TimestampType",
    "timestamp": "TimestampType",
    "time": "StringType",  # Already CAST to CHAR in VIEW
    "blob": "BinaryType",
    "binary": "BinaryType",
    "varbinary": "BinaryType",
    "longblob": "BinaryType",
    "mediumblob": "BinaryType",
    "tinyblob": "BinaryType",
    "bit": "BooleanType",
}

POSTGRES_TO_SPARK_DATA_TYPE = {
    "smallint": "IntegerType",
    "integer": "IntegerType",
    "int": "IntegerType",
    "bigint": "LongType",
    "serial": "IntegerType",
    "bigserial": "LongType",
    "real": "DoubleType",
    "double precision": "DoubleType",
    "numeric": "DecimalType",
    "decimal": "DecimalType",
    "money": "DecimalType",
    "boolean": "BooleanType",
    "bool": "BooleanType",
    "char": "StringType",
    "character": "StringType",
    "varchar": "StringType",
    "character varying": "StringType",
    "text": "StringType",
    "date": "DateType",
    "timestamp": "TimestampType",
    "timestamp without time zone": "TimestampType",
    "timestamp with time zone": "TimestampType",
    "timestamptz": "TimestampType",
    "time": "StringType",  # Already CAST to VARCHAR in VIEW
    "time without time zone": "StringType",
    "time with time zone": "StringType",
    "bytea": "BinaryType",
    "json": "StringType",
    "jsonb": "StringType",
    "uuid": "StringType",
    "xml": "StringType",
    "name": "StringType",
    "bit": "BooleanType",
    "bit varying": "BooleanType",
}

BASE_SPARK_TYPES = {
    "IntegerType": IntegerType,
    "LongType": LongType,
    "ShortType": ShortType,
    "ByteType": ByteType,
    "FloatType": FloatType,
    "DoubleType": DoubleType,
    "DecimalType": DecimalType,
    "StringType": StringType,
    "BooleanType": BooleanType,
    "DateType": DateType,
    "TimestampType": TimestampType,
    "BinaryType": BinaryType,
}


def normalize_base_type(data_type: str) -> str:
    """Normalize data type string to base type name."""
    base = data_type
    pos = base.find("(")
    base = base[:pos] if pos != -1 else base
    base = re.sub(r"\d+", "", base)
    return base.lower().strip()


def convert_source_type_to_spark(data_type: str, source_type: str) -> str:
    """Convert source database type to Spark type name."""
    data_type = normalize_base_type(data_type)
    if source_type == "mysql":
        return MYSQL_TO_SPARK_DATA_TYPE.get(data_type, "StringType")
    elif source_type == "postgres":
        return POSTGRES_TO_SPARK_DATA_TYPE.get(data_type, "StringType")
    else:
        raise NotImplementedError(f"Not implemented for source type: {source_type}")


def convert_spark_type(spark_type_name: str):
    """Get Spark type class from type name."""
    return BASE_SPARK_TYPES.get(spark_type_name, StringType)


def get_source_columns_info(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_driver: str,
    user: str,
    password: str,
    schema: str,
    table: str,
) -> list:
    """Query INFORMATION_SCHEMA from JDBC source to get table schema."""
    query = f"""
        (SELECT
            COLUMN_NAME,
            DATA_TYPE,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION) AS schema_info
    """
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", jdbc_driver)
        .option("user", user)
        .option("password", password)
        .option("dbtable", query)
        .load()
    )

    return [
        {
            "name": row["COLUMN_NAME"],
            "data_type": row["DATA_TYPE"],
            "numeric_precision": row["NUMERIC_PRECISION"],
            "numeric_scale": row["NUMERIC_SCALE"],
        }
        for row in df.collect()
    ]


def build_spark_schema(columns_info: list, source_type: str) -> StructType:
    """Build Spark StructType schema from source columns_info.

    Note: All fields are set to nullable=True to avoid schema mismatch with JDBC driver.
    """
    fields = []
    for col in columns_info:
        col_name = col["name"]
        data_type = col["data_type"]

        spark_type_name = convert_source_type_to_spark(data_type, source_type)

        if spark_type_name == "DecimalType":
            precision = col.get("numeric_precision") or 38
            scale = col.get("numeric_scale") or 18
            spark_type = DecimalType(precision, scale)
        else:
            spark_type_class = convert_spark_type(spark_type_name)
            spark_type = spark_type_class()

        fields.append(StructField(col_name, spark_type, nullable=True))

    return StructType(fields)