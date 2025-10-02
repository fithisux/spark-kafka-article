import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    FloatType,
    TimestampType,
    BooleanType,
)
from pyspark.sql.window import Window

spark = SparkSession.builder.master("spark://spark-master:7077").appName('example').getOrCreate()

BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_PURCHASES = "demo.purchases"

options = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "subscribe": TOPIC_PURCHASES,
    "startingOffsets": "earliest",
    "includeHeaders": "true"
}

df_sales = spark.read.format("kafka").options(**options).load()

schema = StructType(
    [
        StructField("transaction_time", TimestampType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("is_member", BooleanType(), True),
        StructField("member_discount", FloatType(), True),
        StructField("add_supplements", BooleanType(), True),
        StructField("supplement_price", FloatType(), True),
        # StructField("total_purchase", FloatType(), False),
    ]
)

window = Window.partitionBy("product_id").orderBy("quantity")
window_agg = Window.partitionBy("product_id")

(
    df_sales.selectExpr("CAST(value AS STRING)")
    .select(F.from_json("value", schema=schema).alias("data"))
    .select("data.*")
    .withColumn("row", F.row_number().over(window))
    .withColumn("quantity", F.sum(F.col("quantity")).over(window_agg))
    .withColumn("sales", F.sum(F.col("quantity") * F.col("price")).over(window_agg))
    .filter(F.col("row") == 1)
    .drop("row")
    .select(
        "product_id",
        F.format_number("sales", 2).alias("sales"),
        F.format_number("quantity", 0).alias("quantity"),
    )
    .coalesce(1)
    .orderBy(F.regexp_replace("sales", ",", "").cast("float"), ascending=False)
    .show(10)
)