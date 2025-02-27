# Hotel and Weather Data Processing with PySpark

## Overview
This script processes hotel and weather data using **Apache Spark** and integrates geolocation data via the **OpenCage API**. The processed data is then stored in **Azure Data Lake Storage (ADLS)**.

## Dependencies
- Python libraries:
  - `os` (for environment variables)
  - `requests` (for API calls)
  - `pygeohash` (for geohash encoding)
  - `dotenv` (for loading environment variables from `.env` file)
  - `pyspark` (for data processing)
- Azure storage dependencies (Hadoop and Azure SDKs)

## Environment Variables
`.env` file contains these environment variables:

| Variable | Description |
|----------|-------------|
| `AZURE_STORAGE_ACCOUNT` | Azure storage account name |
| `AZURE_CONTAINER_NAME` | Name of the Azure Blob container |
| `AZURE_CONNECTION_STRING` | Azure connection string |
| `OUTPUT_STORAGE_ACCOUNT` | Output storage account for processed data |
| `OUTPUT_CONTAINER_NAME` | Output container name |
| `OUTPUT_PK` | Output storage account key |
| `OPENCAGE_API_KEY` | API key for OpenCage Geocoder |

## Functions

### `get_coordinates(address)`
Fetches latitude and longitude coordinates for a given address using the OpenCage API.

### `calculate_geohash(lat, lon, precision=4)`
Encodes latitude and longitude into a geohash with a default precision of 4.

### `create_spark_session()`
Initializes a Spark session with configurations for Azure Data Lake Storage and credentials.

### `process_data(spark)`
Performs the following operations:
1. Reads hotel data (CSV) and weather data (Parquet) from Azure Blob Storage.
2. Identifies and corrects missing or invalid latitude/longitude values using OpenCage API.
3. Generates **geohashes** for both hotel and weather datasets.
4. Joins weather data with hotel data based on the **geohash**.
5. Writes the final cleaned dataset to **Azure Data Lake Storage**.

## Execution Flow
1. Load environment variables.
2. Validate storage configurations.
3. Create a Spark session.
4. Process hotel and weather data.
5. Save the processed data back to Azure Data Lake Storage.

## Running the Script
Execute the script using:
```bash
python src/main/python/main.py
```
Ensure that Spark and required dependencies are installed before running the script.

## Expected Output
- Processed hotel and weather data stored in Azure Data Lake Storage.

## Unit Tests
This script includes unit tests to verify geolocation functions.

```python
import unittest
from src.main.python.main import get_coordinates, calculate_geohash
import pygeohash as pgh

class TestGeolocationFunctions(unittest.TestCase):

    def test_get_coordinates_valid(self):
        """Test valid address lookup"""
        lat, lon = get_coordinates("New York, USA")
        self.assertIsNotNone(lat)
        self.assertIsNotNone(lon)
        self.assertTrue(-90 <= lat <= 90)
        self.assertTrue(-180 <= lon <= 180)

    def test_get_coordinates_invalid(self):
        """Test invalid address lookup"""
        lat, lon = get_coordinates("SomeNonExistentPlace123")
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_calculate_geohash_valid(self):
        """Test geohash encoding for valid lat/lon"""
        geohash = calculate_geohash(40.7128, -74.0060, precision=4)
        expected = pgh.encode(40.7128, -74.0060, precision=4)
        self.assertEqual(geohash, expected)

    def test_calculate_geohash_invalid(self):
        """Test geohash for invalid lat/lon"""
        self.assertIsNone(calculate_geohash(200, -74.0060))  # Invalid latitude
        self.assertIsNone(calculate_geohash(40.7128, -200))  # Invalid longitude
        self.assertIsNone(calculate_geohash(None, None))  # None values
        self.assertIsNone(calculate_geohash("abc", "xyz"))  # Non-numeric values

if __name__ == "__main__":
    unittest.main()
```

### Running Tests
To run the tests, execute:
```sh
python -m unittest src.test.test_main
```

### Azure storage explorer
Fetching and populating was done with the help of Azure
![azurestorageexplorer](image.png)

Fragment of final output
![fragment](image-1.png)


### Teraform
Default terraform file was used, only the first few lines were modified
```sh
terraform {
  backend "azurerm" {
    resource_group_name  = "sparktraining"
    storage_account_name = "sparktraining01"
    container_name       = "sparktraining"
    key                  = "bdcc.tfstate"
  }
}
```

### Dockerfile
Default dockerfile was used, only a few lines were modified
```sh
COPY dist/sparkbasics-*.whl /opt/
COPY src/main/python/main.py /opt/sparkbasics/

RUN apk update && \
    apk add --no-cache python3 py3-pip && \
    pip3 install --upgrade pip setuptools wheel && \
    pip3 install /opt/sparkbasics-*.whl && \
    rm -rf /var/cache/apk/*
```

### Kubernetes
Terraform created a base for my aks, only one node was used, which had 2 CPU and 8 GB, by the help of 
dockerfile, docker image was created - edgarasdir/sparktraining:v2, which had the necessary dependencies as well as the code
of my program. First I had to create the necessary secret using this command
```sh
kubectl create secret generic my-secret \
  --from-literal=AZURE_STORAGE_ACCOUNT=your_storage_account \
  --from-literal=AZURE_CONTAINER_NAME=your_container_name \
  --from-literal=AZURE_CONNECTION_STRING=your_connection_string \
  --from-literal=OUTPUT_STORAGE_ACCOUNT=your_output_storage_account \
  --from-literal=OUTPUT_CONTAINER_NAME=your_output_container_name \
  --from-literal=OUTPUT_PK=your_output_primary_key \
  --from-literal=OPENCAGE_API_KEY=your_opencage_api_key

```
Then I had to submit my spark application with this command
```sh
spark-submit --master k8s://"my api" --deploy-mode cluster --name spark-job --conf spark.kubernetes.container.image=edgarasdir/sparktraining:v2 --conf spark.kubernetes.driver.secretKeyRef.OPENCAGE_API_KEY=azure-secret:OPENCAGE_API_KEY --conf spark.kubernetes.executor.secretKeyRef.OPENCAGE_API_KEY=azure-secret:OPENCAGE_API_KEY --conf spark.kubernetes.driverEnv.AZURE_STORAGE_ACCOUNT=sparktraining01 --conf spark.kubernetes.executorEnv.AZURE_STORAGE_ACCOUNT=sparktraining01 --conf spark.kubernetes.driverEnv.AZURE_CONTAINER_NAME=sparktraining --conf spark.kubernetes.executorEnv.AZURE_CONTAINER_NAME=sparktraining --conf spark.kubernetes.driver.secretKeyRef.AZURE_SAS_TOKEN=azure-secret:AZURE_SAS_TOKEN --conf spark.kubernetes.executor.secretKeyRef.AZURE_SAS_TOKEN=azure-secret:AZURE_SAS_TOKEN --conf spark.kubernetes.driver.secretKeyRef.AZURE_CONNECTION_STRING=azure-secret:AZURE_CONNECTION_STRING --conf spark.kubernetes.executor.secretKeyRef.AZURE_CONNECTION_STRING=azure-secret:AZURE_CONNECTION_STRING --conf spark.kubernetes.driver.secretKeyRef.OUTPUT_STORAGE_ACCOUNT=azure-secret:OUTPUT_STORAGE_ACCOUNT --conf spark.kubernetes.executor.secretKeyRef.OUTPUT_STORAGE_ACCOUNT=azure-secret:OUTPUT_STORAGE_ACCOUNT --conf spark.kubernetes.driver.secretKeyRef.OUTPUT_CONTAINER_NAME=azure-secret:OUTPUT_CONTAINER_NAME --conf spark.kubernetes.executor.secretKeyRef.OUTPUT_CONTAINER_NAME=azure-secret:OUTPUT_CONTAINER_NAME --conf spark.kubernetes.driver.secretKeyRef.OUTPUT_PK=azure-secret:OUTPUT_PK --conf spark.kubernetes.executor.secretKeyRef.OUTPUT_PK=azure-secret:OUTPUT_PK --conf spark.kubernetes.driver.serviceAccount=spark-job-role-binding --conf spark.kubernetes.executor.serviceAccount=spark-job-role-binding --conf spark.executor.instances=1 --conf spark.executor.memory=1g --conf spark.kubernetes.executor.request.cores=250m --conf spark.kubernetes.driver.request.cores=250m local:///opt/sparkbasics/main.py
```