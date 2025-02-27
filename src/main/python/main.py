import os
import requests
import pygeohash as pgh
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

load_dotenv()

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
container_name = os.getenv("AZURE_CONTAINER_NAME")
connection_string = os.getenv("AZURE_CONNECTION_STRING")
output_storage_account = os.getenv("OUTPUT_STORAGE_ACCOUNT")
output_container_name = os.getenv("OUTPUT_CONTAINER_NAME")
output_pk = os.getenv("OUTPUT_PK")

adls_url = f"abfss://{output_container_name}@{output_storage_account}.dfs.core.windows.net/"

if not storage_account or not container_name or not connection_string:
    raise ValueError("Missing Azure Storage configuration in .env file")


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
    return (lat, lon) if lat is not None and lon is not None else (None, None)

# Convert UDF output to valid coordinates
def calculate_geohash(lat, lon, precision=4):
    try:
        # Convert to float and check if the values are valid
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
        
        # Check if the coordinates are within valid ranges
        if lat is None or lon is None or lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return None
        return pgh.encode(lat, lon, precision)
    except (ValueError, TypeError):
        return None

geohash_udf = udf(calculate_geohash, StringType())

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Hotel Weather Dataframe") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.6") \
        .getOrCreate()

    if "SharedAccessSignature=" in connection_string:
        sas_token = connection_string.split("SharedAccessSignature=")[1].split(";")[0]
    else:
        raise ValueError("ERROR: SAS Token not found in AZURE_CONNECTION_STRING. Check your .env file!")

    spark.conf.set(
        f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net",
        sas_token
    )

    spark.conf.set(f"fs.azure.account.auth.type.{output_storage_account}.dfs.core.windows.net", "SharedKey")
    spark.conf.set(f"fs.azure.account.key.{output_storage_account}.dfs.core.windows.net", output_pk)
    return spark


def process_data(spark):
    # Read CSV file
    hotel_file_path = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/hotels/"
    weather_file_path = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/weather/"
    hotel = spark.read.csv(hotel_file_path, header=True, inferSchema=True)
    weather = spark.read.parquet(weather_file_path)

    # Filter rows with missing or invalid coordinates
    hotel_faulty = hotel.filter(
        col("Latitude").isNull() | col("Longitude").isNull() |  # Check for null values
        (col("Latitude") == "NA") | (col("Longitude") == "NA") |  # Check for "NA" as a string
        (col("Latitude") < -90) | (col("Latitude") > 90) |  # Latitude out of bounds
        (col("Longitude") < -180) | (col("Longitude") > 180)  # Longitude out of bounds
    )

    # Apply UDF to get correct coordinates
    schema = StructType([  # Struct for latitude and longitude
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

    # Drop duplicates based on relevant fields
    weather = weather.dropDuplicates(["avg_tmpr_f", "wthr_date", "Geohash"])

    # Perform the join
    joined_df = weather.join(hotel_final, on="Geohash", how="inner")

    # Write the final result
    dummy_file_path = f"{adls_url}output/"
    joined_df.write.mode("overwrite").parquet(dummy_file_path)

# Main function to run the code
if __name__ == "__main__":
    spark = create_spark_session()
    process_data(spark)
    spark.stop()
