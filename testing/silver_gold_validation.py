from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import shutil


# ============================================================
# CONFIGURATION
# ============================================================

BASE_PATH = "/home/naveen/RetailHub-Spark/data"

SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

OUTPUT_PATH = f"{BASE_PATH}/validation"

# Exclude these datasets
EXCLUDED_DATASETS = {
    "admin_events",
}

# Number of sample rows
SAMPLE_ROWS = 20


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("RetailHub_Silver_Gold_Validation")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# DATASET DISCOVERY
# ============================================================

def discover_datasets(base_path):

    datasets = []

    base = Path(base_path)

    if not base.exists():
        return datasets

    for folder in sorted(base.iterdir()):

        if not folder.is_dir():
            continue

        # Ignore internal/state folders
        if folder.name.startswith("_"):
            continue

        # Exclude unwanted datasets
        if folder.name in EXCLUDED_DATASETS:
            continue

        # Check whether parquet files exist
        parquet_files = list(folder.rglob("*.parquet"))

        if not parquet_files:
            continue

        datasets.append(folder.name)

    return datasets


# ============================================================
# PRINT HEADER
# ============================================================

def print_header(title):

    print("\n")
    print("=" * 120)
    print(title)
    print("=" * 120)


# ============================================================
# INSPECT ONE DATASET
# ============================================================

def inspect_dataset(layer, table_name, table_path):

    print_header(f"{layer.upper()} TABLE : {table_name}")

    print(f"Path       : {table_path}")

    # --------------------------------------------------------
    # READ
    # --------------------------------------------------------

    df = spark.read.parquet(table_path)

    # Force unique column names check
    columns = df.columns

    row_count = df.count()
    column_count = len(columns)

    print(f"Rows       : {row_count}")
    print(f"Columns    : {column_count}")

    # --------------------------------------------------------
    # SCHEMA
    # --------------------------------------------------------

    print("\n")
    print("-" * 120)
    print("FULL SCHEMA")
    print("-" * 120)

    for field in df.schema.fields:

        print(
            f"{field.name:<35} "
            f"{str(field.dataType):<25} "
            f"nullable={field.nullable}"
        )

    # --------------------------------------------------------
    # COLUMN PROFILE
    # --------------------------------------------------------

    print("\n")
    print("-" * 120)
    print("COLUMN PROFILE")
    print("-" * 120)

    print(
        f"{'COLUMN':<35}"
        f"{'TYPE':<18}"
        f"{'ROWS':>10}"
        f"{'NULLS':>10}"
        f"{'NON_NULL':>12}"
        f"{'NULL_%':>10}"
        f"{'DISTINCT':>12}"
        f"{'DUPLICATES':>14}"
    )

    print("-" * 120)

    profile_rows = []

    for field in df.schema.fields:

        col = field.name

        # Null count
        null_count = df.filter(F.col(col).isNull()).count()

        non_null_count = row_count - null_count

        # Distinct values INCLUDING NULL as a value
        distinct_count = df.select(col).distinct().count()

        # Number of duplicated rows for this column
        #
        # Example:
        # A,A,A,B,C
        #
        # A appears 3 times
        # Duplicate occurrences = 2
        #
        duplicate_count = (
            df.groupBy(col)
            .count()
            .filter(F.col("count") > 1)
            .select(
                F.sum(F.col("count") - 1).alias("duplicates")
            )
            .collect()[0]["duplicates"]
        )

        if duplicate_count is None:
            duplicate_count = 0

        null_percentage = (
            (null_count / row_count) * 100
            if row_count > 0
            else 0
        )

        print(
            f"{col:<35}"
            f"{str(field.dataType):<18}"
            f"{row_count:>10}"
            f"{null_count:>10}"
            f"{non_null_count:>12}"
            f"{null_percentage:>9.2f}%"
            f"{distinct_count:>12}"
            f"{duplicate_count:>14}"
        )

        profile_rows.append(
            (
                col,
                str(field.dataType),
                row_count,
                null_count,
                non_null_count,
                float(null_percentage),
                distinct_count,
                int(duplicate_count),
            )
        )

    # --------------------------------------------------------
    # IMPORTANT KEY CHECKS
    # --------------------------------------------------------

    print("\n")
    print("-" * 120)
    print("COMMON KEY / ID CHECKS")
    print("-" * 120)

    key_columns = [
        "event_id",
        "order_id",
        "order_item_id",
        "payment_id",
        "shipment_id",
        "product_id",
        "customer_id",
        "category_id",
        "session_id",
        "cart_id",
        "return_id",
        "review_id",
    ]

    existing_keys = [
        col for col in key_columns
        if col in columns
    ]

    if not existing_keys:

        print("No standard ID columns found.")

    else:

        for col in existing_keys:

            null_count = (
                df.filter(F.col(col).isNull()).count()
            )

            duplicate_groups = (
                df.groupBy(col)
                .count()
                .filter(
                    (F.col("count") > 1) &
                    F.col(col).isNotNull()
                )
                .count()
            )

            duplicate_rows = (
                df.groupBy(col)
                .count()
                .filter(
                    (F.col("count") > 1) &
                    F.col(col).isNotNull()
                )
                .select(
                    F.sum(F.col("count") - 1)
                    .alias("duplicate_rows")
                )
                .collect()[0]["duplicate_rows"]
            )

            if duplicate_rows is None:
                duplicate_rows = 0

            print(
                f"{col:<25}"
                f"NULLS={null_count:<8}"
                f"DUPLICATE_GROUPS={duplicate_groups:<8}"
                f"DUPLICATE_ROWS={duplicate_rows}"
            )

    # --------------------------------------------------------
    # EVENT TYPE DISTRIBUTION
    # --------------------------------------------------------

    if "event_type" in columns:

        print("\n")
        print("-" * 120)
        print("EVENT TYPE DISTRIBUTION")
        print("-" * 120)

        (
            df.groupBy("event_type")
            .count()
            .orderBy(F.desc("count"))
            .show(100, truncate=False)
        )

    # --------------------------------------------------------
    # 20 UNIQUE SAMPLE ROWS
    # --------------------------------------------------------

    print("\n")
    print("-" * 120)
    print("20 UNIQUE SAMPLE ROWS — ALL COLUMNS")
    print("-" * 120)

    #
    # dropDuplicates() ensures the sample rows themselves
    # are unique across ALL columns.
    #

    sample_df = (
        df
        .dropDuplicates()
        .limit(SAMPLE_ROWS)
    )

    sample_count = sample_df.count()

    if sample_count == 0:

        print("No rows available.")

    else:

        sample_df.show(
            SAMPLE_ROWS,
            truncate=False,
            vertical=True
        )

    # --------------------------------------------------------
    # SAVE INDIVIDUAL REPORTS
    # --------------------------------------------------------

    table_output = Path(
        OUTPUT_PATH
    ) / layer / table_name

    table_output.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # SAVE SCHEMA
    # --------------------------------------------------------

    schema_file = table_output / "schema.txt"

    with open(schema_file, "w") as f:

        f.write(
            f"TABLE: {table_name}\n"
        )

        f.write(
            f"LAYER: {layer}\n"
        )

        f.write(
            f"ROWS: {row_count}\n"
        )

        f.write(
            f"COLUMNS: {column_count}\n\n"
        )

        for field in df.schema.fields:

            f.write(
                f"{field.name}\t"
                f"{field.dataType}\t"
                f"nullable={field.nullable}\n"
            )

    # --------------------------------------------------------
    # SAVE COLUMN PROFILE
    # --------------------------------------------------------

    profile_file = table_output / "column_profile.txt"

    with open(profile_file, "w") as f:

        f.write(
            f"{'COLUMN':<35}"
            f"{'TYPE':<18}"
            f"{'ROWS':>10}"
            f"{'NULLS':>10}"
            f"{'NON_NULL':>12}"
            f"{'NULL_%':>10}"
            f"{'DISTINCT':>12}"
            f"{'DUPLICATES':>14}\n"
        )

        f.write("-" * 130 + "\n")

        for row in profile_rows:

            (
                col,
                dtype,
                rows,
                nulls,
                non_null,
                null_pct,
                distinct,
                duplicates,
            ) = row

            f.write(
                f"{col:<35}"
                f"{dtype:<18}"
                f"{rows:>10}"
                f"{nulls:>10}"
                f"{non_null:>12}"
                f"{null_pct:>9.2f}%"
                f"{distinct:>12}"
                f"{duplicates:>14}\n"
            )

    # --------------------------------------------------------
    # SAVE 20 SAMPLE ROWS
    # --------------------------------------------------------

    sample_output = str(
        table_output / "sample_data"
    )

    # Remove previous output
    sample_path = Path(sample_output)

    if sample_path.exists():
        shutil.rmtree(sample_path)

    (
        sample_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(sample_output)
    )

    # --------------------------------------------------------
    # SAVE SUMMARY
    # --------------------------------------------------------

    summary_file = table_output / "summary.txt"

    with open(summary_file, "w") as f:

        f.write(
            f"RetailHub {layer.upper()} TABLE\n"
        )

        f.write("=" * 80 + "\n")

        f.write(
            f"Table       : {table_name}\n"
        )

        f.write(
            f"Path        : {table_path}\n"
        )

        f.write(
            f"Rows        : {row_count}\n"
        )

        f.write(
            f"Columns     : {column_count}\n"
        )

        f.write(
            f"Sample rows : {sample_count}\n"
        )

        f.write("\n")

        f.write(
            "Generated files:\n"
        )

        f.write(
            "  schema.txt\n"
        )

        f.write(
            "  column_profile.txt\n"
        )

        f.write(
            "  sample_data/\n"
        )

    print("\n")
    print(
        f"Reports saved to: {table_output}"
    )


# ============================================================
# DISCOVER SILVER
# ============================================================

silver_datasets = discover_datasets(
    SILVER_PATH
)

gold_datasets = discover_datasets(
    GOLD_PATH
)


# ============================================================
# INVENTORY
# ============================================================

print_header(
    "RETAILHUB SILVER + GOLD DATA VALIDATION"
)

print(
    f"Silver datasets : {len(silver_datasets)}"
)

for table in silver_datasets:

    print(
        f"  SILVER -> {table}"
    )

print()

print(
    f"Gold datasets   : {len(gold_datasets)}"
)

for table in gold_datasets:

    print(
        f"  GOLD   -> {table}"
    )


# ============================================================
# INSPECT SILVER
# ============================================================

for table in silver_datasets:

    inspect_dataset(
        "silver",
        table,
        f"{SILVER_PATH}/{table}"
    )


# ============================================================
# INSPECT GOLD
# ============================================================

for table in gold_datasets:

    inspect_dataset(
        "gold",
        table,
        f"{GOLD_PATH}/{table}"
    )


# ============================================================
# FINISH
# ============================================================

print_header(
    "VALIDATION COMPLETE"
)

print(
    f"Silver tables inspected : {len(silver_datasets)}"
)

print(
    f"Gold tables inspected   : {len(gold_datasets)}"
)

print(
    f"Total tables inspected  : "
    f"{len(silver_datasets) + len(gold_datasets)}"
)

print(
    f"\nIndividual reports are available under:\n"
    f"{OUTPUT_PATH}"
)

spark.stop()