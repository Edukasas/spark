from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HelloWorldApp") \
    .getOrCreate()

# Simple DataFrame
df = spark.createDataFrame([("Hello", "World")], ["Word1", "Word2"])

df.show()

# Stop Spark session
spark.stop()
