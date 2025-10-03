from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("kafka-batch-query").getOrCreate()

BOOTSTRAP_SERVERS = "kafka:29092"
EXAMPLE_TOPIC = "myeexampletopic"

options = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "subscribe": EXAMPLE_TOPIC,
    "startingOffsets": "earliest",
    "includeHeaders": "true"
}

some_df = spark.read.format("kafka").options(**options).load()
some_df.show(10)