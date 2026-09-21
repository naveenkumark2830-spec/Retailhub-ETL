from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    MapType
)

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("simulation_run_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_version", IntegerType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("event_source", StringType(), True),
    StructField("actor_type", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("anonymous_id", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("context", MapType(StringType(), StringType()), True),
    StructField("entity", MapType(StringType(), StringType()), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])