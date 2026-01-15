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
    NullType,
)
from pyspark.sql import SparkSession
import re

# https://apache.github.io/spark/sql-data-sources-jdbc.html
MYSQL_TO_SPARK_DATA_TYPE = {
    # --- Numeric Types ---
    "bit": "BooleanType",
    "tinyint": "ByteType",
    "smallint": "IntegerType",
    "mediumint": "IntegerType",
    "int": "IntegerType",
    "integer": "IntegerType",
    "bigint": "LongType",
    "float": "DoubleType",
    "double": "DoubleType",
    "decimal": "DecimalType",
    "numeric": "DecimalType",
    # --- Date/Time Types ---
    "date": "DateType",
    "datetime": "TimestampType",
    "timestamp": "TimestampType",
    "time": "TimestampType",
    "year": "DateType",
    # --- String/Text Types ---
    "char": "StringType",
    "varchar": "StringType",
    "text": "StringType",
    "tinytext": "StringType",
    "mediumtext": "StringType",
    "longtext": "StringType",
    "json": "StringType",
    "enum": "StringType",
    "set": "StringType",
    # --- Binary Types ---
    "binary": "BinaryType",
    "varbinary": "BinaryType",
    "blob": "BinaryType",
    "tinyblob": "BinaryType",
    "mediumblob": "BinaryType",
    "longblob": "BinaryType",
    "geometry": "BinaryType",
}

POSTGRES_TO_SPARK_DATA_TYPE = {
    # --- Numeric Types ---
    "smallint": "ShortType",
    "smallserial": "ShortType",
    "integer": "IntegerType",
    "serial": "IntegerType",
    "bigint": "LongType",
    "bigserial": "LongType",
    "real": "FloatType",
    "float": "FloatType",
    "double precision": "DoubleType",
    "numeric": "DecimalType",
    "decimal": "DecimalType",
    "money": "StringType",
    "oid": "DecimalType",
    # --- Boolean ---
    "boolean": "BooleanType",
    "bool": "BooleanType",
    "bit": "BooleanType",
    # --- String/Text Types ---
    "character varying": "StringType",
    "varchar": "StringType",
    "character": "StringType",
    "char": "StringType",
    "bpchar": "StringType",
    "text": "StringType",
    "uuid": "StringType",
    "xml": "StringType",
    "json": "StringType",
    "jsonb": "StringType",
    # --- Network/Geo/Other Strings ---
    "inet": "StringType",
    "cidr": "StringType",
    "macaddr": "StringType",
    "point": "StringType",
    "interval": "StringType",
    "tsvector": "StringType",
    # --- Date/Time Types ---
    "date": "DateType",
    "timestamp": "TimestampType",
    "timestamp without time zone": "TimestampType",
    "timestamp with time zone": "TimestampType",
    "timestamptz": "TimestampType",
    "time": "TimestampType",
    "time without time zone": "TimestampType",
    "time with time zone": "TimestampType",
    # --- Binary Types ---
    "bytea": "BinaryType",
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
    "NullType": NullType,
}


def normalize_base_type(data_type):
    """Normalize data type string to base type name."""
    base = data_type
    pos = base.find("(")
    base = base[:pos] if pos != -1 else base
    base = re.sub(r"\d+", "", base)
    return base.lower().strip()


def convert_source_type_to_spark(data_type, source_type):
    """Convert source database type to Spark type name."""
    data_type = normalize_base_type(data_type)
    if source_type == "mysql":
        return MYSQL_TO_SPARK_DATA_TYPE.get(data_type, "StringType")
    elif source_type == "postgres":
        return POSTGRES_TO_SPARK_DATA_TYPE.get(data_type, "StringType")
    else:
        raise NotImplementedError(f"Not implemented for source type: {source_type}")


def convert_spark_type(spark_type_name):
    """Get Spark type class from type name."""
    return BASE_SPARK_TYPES.get(spark_type_name, StringType)


def get_source_columns_info(
    spark: SparkSession,
    jdbc_url,
    jdbc_driver,
    user,
    password,
    schema,
    table,
):
    """Query INFORMATION_SCHEMA from JDBC source to get table schema."""
    query = f"""
        (SELECT
            column_name,
            data_type,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
        ORDER BY ordinal_position) AS schema_info
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

    # Normalize column names to lowercase(postgres)
    df_lower = df.toDF(*[c.lower() for c in df.columns])

    return [
        {
            "name": row["column_name"],
            "data_type": row["data_type"],
            "numeric_precision": row["numeric_precision"],
            "numeric_scale": row["numeric_scale"],
        }
        for row in df_lower.collect()
    ]


def build_spark_schema(columns_info, source_type):
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
