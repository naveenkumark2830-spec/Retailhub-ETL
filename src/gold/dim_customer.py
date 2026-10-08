from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

from dotenv import load_dotenv
import os


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

# Loads /home/naveen/RetailHub-Spark/.env
load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

MYSQL_TABLE = "customers"

from config.settings import GOLD_BASE_PATH

GOLD_PATH = f"{GOLD_BASE_PATH}/dim_customer"

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


# ============================================================
# VALIDATE ENVIRONMENT CONFIGURATION
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
    .appName("RetailHub_Dim_Customer")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

print("\n")
print("=" * 100)
print("RETAILHUB — DIM_CUSTOMER")
print("=" * 100)

print(f"MySQL Host     : {MYSQL_HOST}")
print(f"MySQL Port     : {MYSQL_PORT}")
print(f"MySQL Database : {MYSQL_DATABASE}")
print(f"MySQL Table    : {MYSQL_TABLE}")
print(f"Gold Path      : {GOLD_PATH}")


# ============================================================
# READ CUSTOMERS FROM MYSQL
# ============================================================

print("\n")
print("-" * 100)
print("READING CUSTOMERS FROM MYSQL")
print("-" * 100)

try:

    customers = (
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


source_count = customers.count()

print("\nSuccessfully connected to MySQL.")
print(f"Customers read: {source_count}")


# ============================================================
# SOURCE SCHEMA
# ============================================================

print("\n")
print("-" * 100)
print("MYSQL CUSTOMERS SCHEMA")
print("-" * 100)

customers.printSchema()


# ============================================================
# EXPECTED SOURCE COLUMNS
# ============================================================

EXPECTED_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "gender",
    "country",
    "state",
    "city",
    "language",
    "membership",
    "preferred_payment",
    "account_status",
    "created_at",
    "updated_at",
]


# ============================================================
# CHECK SOURCE COLUMNS
# ============================================================

missing_columns = [
    column
    for column in EXPECTED_COLUMNS
    if column not in customers.columns
]

if missing_columns:

    print("\nERROR — REQUIRED COLUMNS MISSING")

    for column in missing_columns:
        print(f"  {column}")

    spark.stop()

    raise RuntimeError(
        "Required columns missing from MySQL customers table."
    )


# ============================================================
# BUILD DIM_CUSTOMER
# ============================================================
#
# password_hash is intentionally excluded.
#
# It belongs to the application/security layer
# and should not be stored in the analytical dimension.
# ============================================================

dim_customer = customers.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "gender",
    "country",
    "state",
    "city",
    "language",
    "membership",
    "preferred_payment",
    "account_status",
    "created_at",
    "updated_at",
)


# ============================================================
# STANDARDIZE TYPES
# ============================================================

dim_customer = (
    dim_customer

    .withColumn(
        "customer_id",
        F.col("customer_id").cast(StringType())
    )

    .withColumn(
        "first_name",
        F.col("first_name").cast(StringType())
    )

    .withColumn(
        "last_name",
        F.col("last_name").cast(StringType())
    )

    .withColumn(
        "email",
        F.col("email").cast(StringType())
    )

    .withColumn(
        "phone",
        F.col("phone").cast(StringType())
    )

    .withColumn(
        "date_of_birth",
        F.col("date_of_birth").cast(StringType())
    )

    .withColumn(
        "gender",
        F.col("gender").cast(StringType())
    )

    .withColumn(
        "country",
        F.col("country").cast(StringType())
    )

    .withColumn(
        "state",
        F.col("state").cast(StringType())
    )

    .withColumn(
        "city",
        F.col("city").cast(StringType())
    )

    .withColumn(
        "language",
        F.col("language").cast(StringType())
    )

    .withColumn(
        "membership",
        F.col("membership").cast(StringType())
    )

    .withColumn(
        "preferred_payment",
        F.col("preferred_payment").cast(StringType())
    )

    .withColumn(
        "account_status",
        F.col("account_status").cast(StringType())
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
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "gender",
    "country",
    "state",
    "city",
    "language",
    "membership",
    "preferred_payment",
    "account_status",
]


for column in STRING_COLUMNS:

    dim_customer = dim_customer.withColumn(
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

row_count = dim_customer.count()

print(f"Row count : {row_count}")

if row_count == 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — dim_customer is empty."
    )


# ------------------------------------------------------------
# NULL CUSTOMER IDs
# ------------------------------------------------------------

null_customer_ids = (
    dim_customer
    .filter(F.col("customer_id").isNull())
    .count()
)

print(
    f"NULL customer_id rows : {null_customer_ids}"
)


# ------------------------------------------------------------
# DUPLICATE CUSTOMER IDs
# ------------------------------------------------------------

duplicate_customer_ids = (
    dim_customer
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate customer IDs : {duplicate_customer_ids}"
)


# ------------------------------------------------------------
# REQUIRED FIELD CHECKS
# ------------------------------------------------------------

REQUIRED_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "account_status",
]


print("\nRequired field NULL counts:")

dq_failed = False

for column in REQUIRED_COLUMNS:

    null_count = (
        dim_customer
        .filter(F.col(column).isNull())
        .count()
    )

    print(
        f"  {column:<20} : {null_count}"
    )

    if null_count > 0:
        dq_failed = True


# ------------------------------------------------------------
# DUPLICATE EMAIL CHECK
# ------------------------------------------------------------

duplicate_emails = (
    dim_customer
    .filter(F.col("email").isNotNull())
    .groupBy("email")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"\nDuplicate email groups : {duplicate_emails}"
)


# ============================================================
# DISTRIBUTIONS
# ============================================================

print("\nMembership distribution:")

dim_customer.groupBy(
    "membership"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


print("Account status distribution:")

dim_customer.groupBy(
    "account_status"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


print("Country distribution:")

dim_customer.groupBy(
    "country"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


# ============================================================
# SAMPLE CUSTOMERS
# ============================================================

print("\nSample customers:")

dim_customer.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "state",
    "city",
    "membership",
    "account_status"
).dropDuplicates().show(
    20,
    truncate=False
)


# ============================================================
# FAIL FAST IF DQ FAILED
# ============================================================

if null_customer_ids > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — NULL customer_id found."
    )


if duplicate_customer_ids > 0:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — duplicate customer_id found."
    )


if dq_failed:

    spark.stop()

    raise RuntimeError(
        "DATA QUALITY FAILED — required fields contain NULL values."
    )


# ============================================================
# WRITE DIM_CUSTOMER TO GOLD
# ============================================================

print("\n")
print("-" * 100)
print("WRITING DIM_CUSTOMER TO GOLD")
print("-" * 100)

(
    dim_customer
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

written_customer = (
    spark.read
    .parquet(GOLD_PATH)
)


written_count = written_customer.count()


written_null_ids = (
    written_customer
    .filter(F.col("customer_id").isNull())
    .count()
)


written_duplicate_ids = (
    written_customer
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


print(
    f"Source row count            : {row_count}"
)

print(
    f"Gold row count              : {written_count}"
)

print(
    f"Gold NULL customer IDs      : {written_null_ids}"
)

print(
    f"Gold duplicate customer IDs : {written_duplicate_ids}"
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
        "READ-BACK VALIDATION FAILED — NULL customer_id found."
    )


if written_duplicate_ids > 0:

    spark.stop()

    raise RuntimeError(
        "READ-BACK VALIDATION FAILED — duplicate customer_id found."
    )


# ============================================================
# FINAL SUCCESS
# ============================================================

print("\n")
print("=" * 100)
print("DIM_CUSTOMER COMPLETED SUCCESSFULLY")
print("=" * 100)

print(f"Customers processed : {written_count}")
print(f"Gold location       : {GOLD_PATH}")
print("Data quality        : PASS")
print("Read-back validation: PASS")

print("=" * 100)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()