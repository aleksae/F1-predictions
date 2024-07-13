from pyspark.sql import SparkSession
import requests

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark API Data Retrieval") \
    .getOrCreate()

# Define the API endpoint URL
api_url = "https://api.openf1.org/v1/meetings?year=2024"

# Make a GET request to the API
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Convert the data to a Spark DataFrame
    df = spark.createDataFrame(data)
    
    # Show the DataFrame schema and first few rows
    df.printSchema()
    df.show()
else:
    print("Failed to retrieve data from the API. Status code:", response.status_code)

# Stop Spark session
spark.stop()
