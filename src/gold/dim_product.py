from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    TimestampType
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

MYSQL_TABLE = "products"

from config.settings import GOLD_BASE_PATH

GOLD_PATH = f"{GOLD_BASE_PATH}/dim_product"

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
# JDBC
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
    .appName("RetailHub_Dim_Product")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

print("\n")
print("=" * 100)
print("RETAILHUB — DIM_PRODUCT")
print("=" * 100)

print(f"MySQL Host     : {MYSQL_HOST}")
print(f"MySQL Port     : {MYSQL_PORT}")
print(f"MySQL Database : {MYSQL_DATABASE}")
print(f"MySQL Table    : {MYSQL_TABLE}")
print(f"Gold Path      : {GOLD_PATH}")


# ============================================================
# READ PRODUCTS FROM MYSQL
# ============================================================

print("\n")
print("-" * 100)
print("READING PRODUCTS FROM MYSQL")
print("-" * 100)

try:

    products = (
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


source_count = products.count()

print("\nSuccessfully connected to MySQL.")
print(f"Products read: {source_count}")


# ============================================================
# SOURCE SCHEMA
# ============================================================

print("\n")
print("-" * 100)
print("MYSQL PRODUCTS SCHEMA")
print("-" * 100)

products.printSchema()


# ============================================================
# EXPECTED COLUMNS
# ============================================================

EXPECTED_COLUMNS = [
    "product_id",
    "sku",
    "name",
    "brand",
    "category_id",
    "subcategory_id",
    "description",
    "price",
    "discount",
    "sale_price",
    "rating",
    "review_count",
    "weight",
    "color",
    "size",
    "warranty",
    "return_eligible",
    "country",
    "delivery_days",
    "status",
    "created_at",
    "updated_at",
]


# ============================================================
# CHECK SOURCE COLUMNS
# ============================================================

missing_columns = [
    column
    for column in EXPECTED_COLUMNS
    if column not in products.columns
]

if missing_columns:

    print("\nERROR — REQUIRED COLUMNS MISSING")

    for column in missing_columns:
        print(f"  {column}")

    spark.stop()

    raise RuntimeError(
        "Required columns missing from MySQL products table."
    )


# ============================================================
# BUILD DIM_PRODUCT
# ============================================================

dim_product = products.select(
    "product_id",
    "sku",
    "name",
    "brand",
    "category_id",
    "subcategory_id",
    "description",
    "price",
    "discount",
    "sale_price",
    "rating",
    "review_count",
    "weight",
    "color",
    "size",
    "warranty",
    "return_eligible",
    "country",
    "delivery_days",
    "status",
    "created_at",
    "updated_at",
)


# ============================================================
# STANDARDIZE TYPES
# ============================================================

dim_product = (
    dim_product

    .withColumn(
        "product_id",
        F.col("product_id").cast(StringType())
    )

    .withColumn(
        "sku",
        F.col("sku").cast(StringType())
    )

    .withColumn(
        "name",
        F.col("name").cast(StringType())
    )

    .withColumn(
        "brand",
        F.col("brand").cast(StringType())
    )

    .withColumn(
        "category_id",
        F.col("category_id").cast(StringType())
    )

    .withColumn(
        "subcategory_id",
        F.col("subcategory_id").cast(StringType())
    )

    .withColumn(
        "description",
        F.col("description").cast(StringType())
    )

    .withColumn(
        "price",
        F.col("price").cast(DoubleType())
    )

    .withColumn(
        "discount",
        F.col("discount").cast(DoubleType())
    )

    .withColumn(
        "sale_price",
        F.col("sale_price").cast(DoubleType())
    )

    .withColumn(
        "rating",
        F.col("rating").cast(DoubleType())
    )

    .withColumn(
        "review_count",
        F.col("review_count").cast(IntegerType())
    )

    .withColumn(
        "weight",
        F.col("weight").cast(DoubleType())
    )

    .withColumn(
        "color",
        F.col("color").cast(StringType())
    )

    .withColumn(
        "size",
        F.col("size").cast(StringType())
    )

    .withColumn(
        "warranty",
        F.col("warranty").cast(StringType())
    )

    .withColumn(
        "return_eligible",
        F.col("return_eligible").cast(BooleanType())
    )

    .withColumn(
        "country",
        F.col("country").cast(StringType())
    )

    .withColumn(
        "delivery_days",
        F.col("delivery_days").cast(IntegerType())
    )

    .withColumn(
        "status",
        F.col("status").cast(StringType())
    )

    .withColumn(
        "created_at",
        F.col("created_at").cast(TimestampType())
    )

    .withColumn(
        "updated_at",
        F.col("updated_at").cast(TimestampType())
    )
)


# ============================================================
# CLEAN STRING VALUES
# ============================================================

STRING_COLUMNS = [
    "product_id",
    "sku",
    "name",
    "brand",
    "category_id",
    "subcategory_id",
    "description",
    "color",
    "size",
    "warranty",
    "country",
    "status",
]


for column in STRING_COLUMNS:

    dim_product = dim_product.withColumn(
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

row_count = dim_product.count()

print(f"Row count : {row_count}")

if row_count == 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — dim_product is empty."
    )


# ------------------------------------------------------------
# NULL PRODUCT IDs
# ------------------------------------------------------------

null_product_ids = (
    dim_product
    .filter(F.col("product_id").isNull())
    .count()
)

print(
    f"NULL product_id rows : {null_product_ids}"
)


# ------------------------------------------------------------
# DUPLICATE PRODUCT IDs
# ------------------------------------------------------------

duplicate_product_ids = (
    dim_product
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate product IDs : {duplicate_product_ids}"
)


# ------------------------------------------------------------
# DUPLICATE SKU
# ------------------------------------------------------------

duplicate_skus = (
    dim_product
    .filter(F.col("sku").isNotNull())
    .groupBy("sku")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate SKU groups : {duplicate_skus}"
)


# ------------------------------------------------------------
# REQUIRED FIELD CHECKS
# ------------------------------------------------------------

REQUIRED_COLUMNS = [
    "product_id",
    "sku",
    "name",
    "category_id",
    "price",
    "status",
]


print("\nRequired field NULL counts:")

dq_failed = False

for column in REQUIRED_COLUMNS:

    null_count = (
        dim_product
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

negative_price = (
    dim_product
    .filter(
        F.col("price").isNotNull() &
        (F.col("price") < 0)
    )
    .count()
)

negative_sale_price = (
    dim_product
    .filter(
        F.col("sale_price").isNotNull() &
        (F.col("sale_price") < 0)
    )
    .count()
)

invalid_rating = (
    dim_product
    .filter(
        F.col("rating").isNotNull() &
        (
            (F.col("rating") < 0) |
            (F.col("rating") > 5)
        )
    )
    .count()
)

negative_review_count = (
    dim_product
    .filter(
        F.col("review_count").isNotNull() &
        (F.col("review_count") < 0)
    )
    .count()
)

negative_delivery_days = (
    dim_product
    .filter(
        F.col("delivery_days").isNotNull() &
        (F.col("delivery_days") < 0)
    )
    .count()
)


print("\nNumeric validation:")

print(
    f"  Negative price rows       : {negative_price}"
)

print(
    f"  Negative sale price rows  : {negative_sale_price}"
)

print(
    f"  Invalid rating rows       : {invalid_rating}"
)

print(
    f"  Negative review counts    : {negative_review_count}"
)

print(
    f"  Negative delivery days    : {negative_delivery_days}"
)


if (
    negative_price > 0
    or negative_sale_price > 0
    or invalid_rating > 0
    or negative_review_count > 0
    or negative_delivery_days > 0
):

    dq_failed = True


# ============================================================
# DISTRIBUTIONS
# ============================================================

print("\nProduct status distribution:")

dim_product.groupBy(
    "status"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


print("Category distribution:")

dim_product.groupBy(
    "category_id"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


print("Brand distribution:")

dim_product.groupBy(
    "brand"
).count().orderBy(
    F.desc("count")
).show(
    20,
    truncate=False
)


print("Country distribution:")

dim_product.groupBy(
    "country"
).count().orderBy(
    F.desc("count")
).show(
    20,
    truncate=False
)


# ============================================================
# SAMPLE PRODUCTS
# ============================================================

print("\nSample products:")

dim_product.select(
    "product_id",
    "sku",
    "name",
    "brand",
    "category_id",
    "price",
    "sale_price",
    "rating",
    "status"
).dropDuplicates().show(
    20,
    truncate=False
)


# ============================================================
# FAIL FAST
# ============================================================

if null_product_ids > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — NULL product_id found."
    )


if duplicate_product_ids > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate product_id found."
    )


if duplicate_skus > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate SKU found."
    )


if dq_failed:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — product dimension validation failed."
    )


# ============================================================
# WRITE TO GOLD
# ============================================================

print("\n")
print("-" * 100)
print("WRITING DIM_PRODUCT TO GOLD")
print("-" * 100)

(
    dim_product
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

written_product = (
    spark.read
    .parquet(GOLD_PATH)
)

written_count = written_product.count()

written_null_ids = (
    written_product
    .filter(F.col("product_id").isNull())
    .count()
)

written_duplicate_ids = (
    written_product
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


print(
    f"Source row count             : {row_count}"
)

print(
    f"Gold row count               : {written_count}"
)

print(
    f"Gold NULL product IDs        : {written_null_ids}"
)

print(
    f"Gold duplicate product IDs   : {written_duplicate_ids}"
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
        "READ-BACK VALIDATION FAILED — NULL product_id found."
    )


if written_duplicate_ids > 0:

    spark.stop()

    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — duplicate product_id found."
    )


# ============================================================
# SUCCESS
# ============================================================

print("\n")
print("=" * 100)
print("DIM_PRODUCT COMPLETED SUCCESSFULLY")
print("=" * 100)

print(
    f"Products processed : {written_count}"
)

print(
    f"Gold location      : {GOLD_PATH}"
)

print("Data quality       : PASS")
print("Read-back validation: PASS")

print("=" * 100)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()