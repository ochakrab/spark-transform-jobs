from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark to Redshift COPY with Data Type Conversion") \
    .getOrCreate()

# Define the expected Redshift schema
expected_schema = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True)
])

# Sample DataFrame creation with initial data types
data = [("Alice", "1"), ("Bob", "2"), ("Cathy", "3")]  # IDs are strings initially
df = spark.createDataFrame(data, schema=expected_schema)

# Function to convert DataFrame columns based on expected schema
def convert_data_types(df, expected_schema):
    for field in expected_schema.fields:
        if field.name in df.columns:
            if df.schema[field.name].dataType != field.dataType:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df

# Convert DataFrame columns to match Redshift schema
df_converted = convert_data_types(df, expected_schema)

# Define S3 bucket and file path
s3_bucket = "s3://<YOUR_BUCKET_NAME>/data/"
s3_file_path = s3_bucket + "data_file.csv"

# Write DataFrame to S3 as CSV
df_converted.write.csv(s3_file_path, header=True)

# Redshift connection properties
redshift_endpoint = "<REDSHIFT_ENDPOINT>"
database = "<DATABASE>"
user = "<USERNAME>"
password = "<PASSWORD>"
table_name = "<TABLE_NAME>"

# Copy command to load data into Redshift
copy_command = f"""
COPY {table_name}
FROM '{s3_file_path}'
CREDENTIALS 'aws_access_key_id=<YOUR_ACCESS_KEY>;aws_secret_access_key=<YOUR_SECRET_KEY>'
CSV
IGNOREHEADER 1
REGION 'us-west-2';  -- Change this to your region
"""

# Create a connection to Redshift using psycopg2
import psycopg2

try:
    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=redshift_endpoint,
        port='5439'
    )
    
    # Create a cursor
    cursor = conn.cursor()

    # Execute the COPY command
    cursor.execute(copy_command)

    # Commit the transaction
    conn.commit()
    
    print("Data copied to Redshift successfully.")
    
except Exception as e:
    print(f"Error: {e}")
finally:
    # Close cursor and connection
    cursor.close()
    conn.close()

# Stop Spark session
spark.stop()
