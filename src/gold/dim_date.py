from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os


# ============================================================
# LOAD .ENV
# ============================================================

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

from config.settings import GOLD_BASE_PATH

GOLD_PATH = f"{GOLD_BASE_PATH}/dim_date"

START_DATE = "2020-01-01"
END_DATE = "2030-12-31"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub_Dim_Date")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

print("\n")
print("=" * 100)
print("RETAILHUB — DIM_DATE")
print("=" * 100)

print(f"Start Date : {START_DATE}")
print(f"End Date   : {END_DATE}")
print(f"Gold Path  : {GOLD_PATH}")


# ============================================================
# GENERATE DATE RANGE
# ============================================================

print("\n")
print("-" * 100)
print("GENERATING DATE DIMENSION")
print("-" * 100)

date_df = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(START_DATE)),
                F.to_date(F.lit(END_DATE)),
                F.expr("interval 1 day")
            )
        ).alias("date")
    )
)


# ============================================================
# BUILD DIM_DATE
# ============================================================

dim_date = (
    date_df

    # Surrogate key: YYYYMMDD
    .withColumn(
        "date_key",
        F.date_format(
            F.col("date"),
            "yyyyMMdd"
        ).cast("int")
    )

    .withColumn(
        "year",
        F.year("date")
    )

    .withColumn(
        "quarter",
        F.quarter("date")
    )

    .withColumn(
        "month",
        F.month("date")
    )

    .withColumn(
        "month_name",
        F.date_format(
            "date",
            "MMMM"
        )
    )

    .withColumn(
        "week_of_year",
        F.weekofyear("date")
    )

    .withColumn(
        "day",
        F.dayofmonth("date")
    )

    .withColumn(
        "day_of_week",
        F.dayofweek("date")
    )

    .withColumn(
        "day_name",
        F.date_format(
            "date",
            "EEEE"
        )
    )

    .withColumn(
        "is_weekend",
        F.dayofweek("date").isin([1, 7])
    )

    .withColumn(
        "year_month",
        F.date_format(
            "date",
            "yyyy-MM"
        )
    )

    .withColumn(
        "year_quarter",
        F.concat(
            F.col("year"),
            F.lit("-Q"),
            F.col("quarter")
        )
    )
)


# ============================================================
# COLUMN ORDER
# ============================================================

dim_date = dim_date.select(
    "date_key",
    "date",
    "year",
    "quarter",
    "month",
    "month_name",
    "week_of_year",
    "day",
    "day_of_week",
    "day_name",
    "is_weekend",
    "year_month",
    "year_quarter"
)


# ============================================================
# SCHEMA
# ============================================================

print("\n")
print("-" * 100)
print("DIM_DATE SCHEMA")
print("-" * 100)

dim_date.printSchema()


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\n")
print("-" * 100)
print("DATA QUALITY CHECKS")
print("-" * 100)


row_count = dim_date.count()

print(f"Row count : {row_count}")


# ------------------------------------------------------------
# NULL DATE CHECK
# ------------------------------------------------------------

null_dates = (
    dim_date
    .filter(F.col("date").isNull())
    .count()
)

print(f"NULL date rows : {null_dates}")


# ------------------------------------------------------------
# NULL DATE KEY CHECK
# ------------------------------------------------------------

null_date_keys = (
    dim_date
    .filter(F.col("date_key").isNull())
    .count()
)

print(f"NULL date_key rows : {null_date_keys}")


# ------------------------------------------------------------
# DUPLICATE DATE CHECK
# ------------------------------------------------------------

duplicate_dates = (
    dim_date
    .groupBy("date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate dates : {duplicate_dates}")


# ------------------------------------------------------------
# DUPLICATE DATE KEY CHECK
# ------------------------------------------------------------

duplicate_date_keys = (
    dim_date
    .groupBy("date_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate date_keys : {duplicate_date_keys}")


# ============================================================
# DATE RANGE VALIDATION
# ============================================================

actual_min_date = (
    dim_date
    .select(F.min("date"))
    .first()[0]
)

actual_max_date = (
    dim_date
    .select(F.max("date"))
    .first()[0]
)

print("\nDate range:")
print(f"Minimum date : {actual_min_date}")
print(f"Maximum date : {actual_max_date}")


# ============================================================
# WEEKEND DISTRIBUTION
# ============================================================

print("\nWeekend distribution:")

dim_date.groupBy(
    "is_weekend"
).count().orderBy(
    "is_weekend"
).show()


# ============================================================
# SAMPLE DATES
# ============================================================

print("Sample dates:")

dim_date.orderBy(
    "date"
).show(
    20,
    truncate=False
)


# ============================================================
# FAIL FAST
# ============================================================

if row_count == 0:
    spark.stop()
    raise RuntimeError(
        "DATA QUALITY FAILED — dim_date is empty."
    )


if null_dates > 0:
    spark.stop()
    raise RuntimeError(
        "DATA QUALITY FAILED — NULL date found."
    )


if null_date_keys > 0:
    spark.stop()
    raise RuntimeError(
        "DATA QUALITY FAILED — NULL date_key found."
    )


if duplicate_dates > 0:
    spark.stop()
    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate date found."
    )


if duplicate_date_keys > 0:
    spark.stop()
    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate date_key found."
    )


# ============================================================
# WRITE TO GOLD
# ============================================================

print("\n")
print("-" * 100)
print("WRITING DIM_DATE TO GOLD")
print("-" * 100)

(
    dim_date
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

print(f"Gold location: {GOLD_PATH}")


# ============================================================
# READ-BACK VALIDATION
# ============================================================

print("\n")
print("-" * 100)
print("READ-BACK VALIDATION")
print("-" * 100)

written_date = (
    spark.read
    .parquet(GOLD_PATH)
)

written_count = written_date.count()

written_null_dates = (
    written_date
    .filter(F.col("date").isNull())
    .count()
)

written_duplicate_keys = (
    written_date
    .groupBy("date_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Source row count          : {row_count}")
print(f"Gold row count            : {written_count}")
print(f"Gold NULL dates           : {written_null_dates}")
print(f"Gold duplicate date_keys  : {written_duplicate_keys}")


# ============================================================
# FINAL VALIDATION
# ============================================================

if written_count != row_count:
    spark.stop()
    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — row count mismatch."
    )


if written_null_dates > 0:
    spark.stop()
    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — NULL dates found."
    )


if written_duplicate_keys > 0:
    spark.stop()
    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — duplicate date_keys found."
    )


# ============================================================
# SUCCESS
# ============================================================

print("\n")
print("=" * 100)
print("DIM_DATE COMPLETED SUCCESSFULLY")
print("=" * 100)

print(f"Dates processed     : {written_count}")
print(f"Gold location       : {GOLD_PATH}")
print("Data quality        : PASS")
print("Read-back validation: PASS")

print("=" * 100)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()