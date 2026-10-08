from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType
)

from dotenv import load_dotenv
import os


# ============================================================
# LOAD .ENV
# ============================================================

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

MYSQL_TABLE = "categories"

from config.settings import GOLD_BASE_PATH

GOLD_PATH = f"{GOLD_BASE_PATH}/dim_category"

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


# ============================================================
# VALIDATE ENVIRONMENT
# ============================================================

required_env = {
    "MYSQL_HOST": MYSQL_HOST,
    "MYSQL_PORT": MYSQL_PORT,
    "MYSQL_DATABASE": MYSQL_DATABASE,
    "MYSQL_USER": MYSQL_USER,
    "MYSQL_PASSWORD": MYSQL_PASSWORD,
}

missing_env = [
    key
    for key, value in required_env.items()
    if value is None or value == ""
]

if missing_env:

    print("\nERROR — REQUIRED ENVIRONMENT VARIABLES MISSING")

    for variable in missing_env:
        print(f"  {variable}")

    raise RuntimeError(
        "Missing required variables in .env file."
    )


# ============================================================
# JDBC URL
# ============================================================

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    f"?useSSL=false"
    f"&allowPublicKeyRetrieval=true"
    f"&serverTimezone=UTC"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub_Dim_Category")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

print("\n")
print("=" * 100)
print("RETAILHUB — DIM_CATEGORY")
print("=" * 100)

print(f"MySQL Host     : {MYSQL_HOST}")
print(f"MySQL Port     : {MYSQL_PORT}")
print(f"MySQL Database : {MYSQL_DATABASE}")
print(f"MySQL Table    : {MYSQL_TABLE}")
print(f"Gold Path      : {GOLD_PATH}")


# ============================================================
# READ CATEGORIES FROM MYSQL
# ============================================================

print("\n")
print("-" * 100)
print("READING CATEGORIES FROM MYSQL")
print("-" * 100)

try:

    categories = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", MYSQL_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .load()
    )

except Exception as e:

    print("\nMYSQL CONNECTION FAILED")
    print(str(e))

    spark.stop()

    raise


source_count = categories.count()

print("\nSuccessfully connected to MySQL.")
print(f"Categories read: {source_count}")


# ============================================================
# SOURCE SCHEMA
# ============================================================

print("\n")
print("-" * 100)
print("MYSQL CATEGORIES SCHEMA")
print("-" * 100)

categories.printSchema()


# ============================================================
# EXPECTED COLUMNS
# ============================================================

EXPECTED_COLUMNS = [
    "category_id",
    "name",
    "slug",
    "description",
    "image_url",
    "product_count",
    "display_order",
    "status",
]


# ============================================================
# CHECK SOURCE COLUMNS
# ============================================================

missing_columns = [
    column
    for column in EXPECTED_COLUMNS
    if column not in categories.columns
]

if missing_columns:

    print("\nERROR — REQUIRED COLUMNS MISSING")

    for column in missing_columns:
        print(f"  {column}")

    spark.stop()

    raise RuntimeError(
        "Required columns missing from MySQL categories table."
    )


# ============================================================
# BUILD DIM_CATEGORY
# ============================================================

dim_category = categories.select(
    "category_id",
    "name",
    "slug",
    "description",
    "image_url",
    "product_count",
    "display_order",
    "status",
)


# ============================================================
# STANDARDIZE TYPES
# ============================================================

dim_category = (
    dim_category

    .withColumn(
        "category_id",
        F.col("category_id").cast(StringType())
    )

    .withColumn(
        "name",
        F.col("name").cast(StringType())
    )

    .withColumn(
        "slug",
        F.col("slug").cast(StringType())
    )

    .withColumn(
        "description",
        F.col("description").cast(StringType())
    )

    .withColumn(
        "image_url",
        F.col("image_url").cast(StringType())
    )

    .withColumn(
        "product_count",
        F.col("product_count").cast(IntegerType())
    )

    .withColumn(
        "display_order",
        F.col("display_order").cast(IntegerType())
    )

    .withColumn(
        "status",
        F.col("status").cast(StringType())
    )
)


# ============================================================
# CLEAN STRING VALUES
# ============================================================

STRING_COLUMNS = [
    "category_id",
    "name",
    "slug",
    "description",
    "image_url",
    "status",
]


for column in STRING_COLUMNS:

    dim_category = dim_category.withColumn(
        column,
        F.when(
            F.trim(F.col(column)) == "",
            None
        ).otherwise(
            F.trim(F.col(column))
        )
    )


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\n")
print("-" * 100)
print("DATA QUALITY CHECKS")
print("-" * 100)


# ------------------------------------------------------------
# ROW COUNT
# ------------------------------------------------------------

row_count = dim_category.count()

print(f"Row count : {row_count}")

if row_count == 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — dim_category is empty."
    )


# ------------------------------------------------------------
# NULL CATEGORY IDs
# ------------------------------------------------------------

null_category_ids = (
    dim_category
    .filter(F.col("category_id").isNull())
    .count()
)

print(
    f"NULL category_id rows : {null_category_ids}"
)


# ------------------------------------------------------------
# DUPLICATE CATEGORY IDs
# ------------------------------------------------------------

duplicate_category_ids = (
    dim_category
    .groupBy("category_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate category IDs : {duplicate_category_ids}"
)


# ------------------------------------------------------------
# DUPLICATE SLUGS
# ------------------------------------------------------------

duplicate_slugs = (
    dim_category
    .filter(F.col("slug").isNotNull())
    .groupBy("slug")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate slug groups : {duplicate_slugs}"
)


# ------------------------------------------------------------
# REQUIRED FIELD CHECKS
# ------------------------------------------------------------

REQUIRED_COLUMNS = [
    "category_id",
    "name",
    "slug",
    "status",
]


print("\nRequired field NULL counts:")

dq_failed = False

for column in REQUIRED_COLUMNS:

    null_count = (
        dim_category
        .filter(F.col(column).isNull())
        .count()
    )

    print(
        f"  {column:<20} : {null_count}"
    )

    if null_count > 0:
        dq_failed = True


# ------------------------------------------------------------
# NUMERIC VALIDATION
# ------------------------------------------------------------

negative_product_count = (
    dim_category
    .filter(
        F.col("product_count").isNotNull() &
        (F.col("product_count") < 0)
    )
    .count()
)

negative_display_order = (
    dim_category
    .filter(
        F.col("display_order").isNotNull() &
        (F.col("display_order") < 0)
    )
    .count()
)


print("\nNumeric validation:")

print(
    f"  Negative product_count rows : {negative_product_count}"
)

print(
    f"  Negative display_order rows  : {negative_display_order}"
)


if (
    negative_product_count > 0
    or negative_display_order > 0
):

    dq_failed = True


# ============================================================
# DISTRIBUTIONS
# ============================================================

print("\nCategory status distribution:")

dim_category.groupBy(
    "status"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


# ============================================================
# PRODUCT COUNT SUMMARY
# ============================================================

print("Product count summary:")

dim_category.select(
    F.min("product_count").alias("min_products"),
    F.max("product_count").alias("max_products"),
    F.avg("product_count").alias("avg_products"),
    F.sum("product_count").alias("total_products")
).show(
    truncate=False
)


# ============================================================
# SAMPLE CATEGORIES
# ============================================================

print("\nSample categories:")

dim_category.select(
    "category_id",
    "name",
    "slug",
    "product_count",
    "display_order",
    "status"
).dropDuplicates().show(
    20,
    truncate=False
)


# ============================================================
# FAIL FAST
# ============================================================

if null_category_ids > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — NULL category_id found."
    )


if duplicate_category_ids > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate category_id found."
    )


if duplicate_slugs > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate slug found."
    )


if dq_failed:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — category dimension validation failed."
    )


# ============================================================
# WRITE TO GOLD
# ============================================================

print("\n")
print("-" * 100)
print("WRITING DIM_CATEGORY TO GOLD")
print("-" * 100)

(
    dim_category
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

print(
    f"Gold location: {GOLD_PATH}"
)


# ============================================================
# READ-BACK VALIDATION
# ============================================================

print("\n")
print("-" * 100)
print("READ-BACK VALIDATION")
print("-" * 100)

written_category = (
    spark.read
    .parquet(GOLD_PATH)
)

written_count = written_category.count()

written_null_ids = (
    written_category
    .filter(F.col("category_id").isNull())
    .count()
)

written_duplicate_ids = (
    written_category
    .groupBy("category_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


print(
    f"Source row count              : {row_count}"
)

print(
    f"Gold row count                : {written_count}"
)

print(
    f"Gold NULL category IDs        : {written_null_ids}"
)

print(
    f"Gold duplicate category IDs   : {written_duplicate_ids}"
)


# ============================================================
# FINAL VALIDATION
# ============================================================

if written_count != row_count:

    spark.stop()

    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — row count mismatch."
    )


if written_null_ids > 0:

    spark.stop()

    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — NULL category_id found."
    )


if written_duplicate_ids > 0:

    spark.stop()

    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — duplicate category_id found."
    )


# ============================================================
# SUCCESS
# ============================================================

print("\n")
print("=" * 100)
print("DIM_CATEGORY COMPLETED SUCCESSFULLY")
print("=" * 100)

print(
    f"Categories processed : {written_count}"
)

print(
    f"Gold location        : {GOLD_PATH}"
)

print("Data quality         : PASS")
print("Read-back validation : PASS")

print("=" * 100)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()