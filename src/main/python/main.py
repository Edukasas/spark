import os
import requests
import pygeohash as pgh
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

load_dotenv()

API_KEY = os.getenv("OPENCAGE_API_KEY")

if not API_KEY:
    raise ValueError("API key not found! Make sure .env file is properly set up.")
# Function to get coordinates from OpenCage API
def get_coordinates(address):
    url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={API_KEY}"
    response = requests.get(url)
    data = response.json()
    
    if data["results"]:
        lat = data["results"][0]["geometry"]["lat"]
        lon = data["results"][0]["geometry"]["lng"]
        return lat, lon
    return None, None  # Return None if no result found

# Convert function to PySpark UDF
def get_coordinates_udf(address):
    lat, lon = get_coordinates(address)
    return (lat, lon)

def calculate_geohash(lat, lon, precision=4):
    try:
        # Convert to float and check if the values are valid
        lat = float(lat)
        lon = float(lon)
        
        # Check if the coordinates are within valid ranges
        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return None
        return pgh.encode(lat, lon, precision)
    except (ValueError, TypeError):
        return None

geohash_udf = udf(calculate_geohash, StringType())
# Initialize Spark session

spark = SparkSession.builder \
    .appName("Weather Deduplication") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "16g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# Read CSV file
hotel = spark.read.csv(
    r"C:\Users\Edas\Downloads\m06sparkbasics\m06sparkbasics\hotels",
    header=True,
    inferSchema=True
)

weather = spark.read.parquet(
    r"C:\Users\Edas\Downloads\m06sparkbasics\m06sparkbasics\weather"
).limit(100000)

# Filter rows with missing or invalid coordinates
hotel_faulty = hotel.filter(
    col("Latitude").isNull() | col("Longitude").isNull() |  # Check for null values
    (col("Latitude") == "NA") | (col("Longitude") == "NA") |  # Check for "NA" as a string
    (col("Latitude") < -90) | (col("Latitude") > 90) |  # Latitude out of bounds
    (col("Longitude") < -180) | (col("Longitude") > 180)  # Longitude out of bounds
)

# Apply UDF to get correct coordinates
schema = StructType([
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True)
])

get_coordinates_spark_udf = udf(get_coordinates_udf, schema)

# Apply UDF to update faulty coordinates
hotel_fixed = hotel_faulty.withColumn("CorrectedCoords", get_coordinates_spark_udf(col("Address")))

# Split CorrectedCoords into Latitude and Longitude
hotel_fixed = hotel_fixed.withColumn("Latitude", col("CorrectedCoords.Latitude")) \
                         .withColumn("Longitude", col("CorrectedCoords.Longitude")) \
                         .drop("CorrectedCoords")

hotel_valid = hotel.subtract(hotel_faulty)

hotel_final = hotel_valid.union(hotel_fixed)

hotel_final = hotel_final.withColumn("Geohash", geohash_udf(col("Latitude"), col("Longitude")))

hotel_final = hotel_final.drop("Latitude", "Longitude")

weather = weather.withColumn("Geohash", geohash_udf(col("lat"), col("lng")))

weather = weather.drop("lat", "lng")

weather = weather.persist()

weather = weather.dropDuplicates(["avg_tmpr_f", "wthr_date", "Geohash"])

joined_df = weather.join(hotel_final, on="Geohash", how="inner")

joined_df.show()
# Stop Spark session
spark.stop()
