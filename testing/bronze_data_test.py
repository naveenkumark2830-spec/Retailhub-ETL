from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ViewBronze")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(
    "/home/naveen/RetailHub-Spark/data/bronze"
)

df.printSchema()

df.show(20, truncate=False)