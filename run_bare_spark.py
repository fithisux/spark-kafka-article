# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder.master("spark://spark-master:7077").appName('bare-run').getOrCreate()

df = spark.range(100)

df.write.mode("overwrite").parquet("blablah.parquet")

df.show()

spark.stop()