import sys
import boto3
from pyspark.sql import SparkSession
from delta import *

# Initialize a Spark session with Delta support
spark = SparkSession.builder \
    .appName("CreateDeltaTable") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define S3 bucket and path
s3_bucket = "your-s3-bucket-name"
s3_path = f"s3://{s3_bucket}/delta-table/"

# Create some sample data
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
columns = ["id", "name"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Write the DataFrame as a Delta table
df.write.format("delta").mode("overwrite").save(s3_path)

# Create a Glue client
glue = boto3.client('glue')

# Define the Glue table parameters
table_name = "your_delta_table_name"
database_name = "your_database_name"

# Create a new Glue table
glue.create_table(
    DatabaseName=database_name,
    TableInput={
        'Name': table_name,
        'Description': 'Delta table created from Spark',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'id', 'Type': 'int'},
                {'Name': 'name', 'Type': 'string'}
            ],
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.HiveDeltaInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveDeltaOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.spark.sql.delta.serialization.DeltaSerDe'
            },
            'BucketColumns': [],
            'SortColumns': [],
            'Parameters': {
                'classification': 'delta'
            }
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'classification': 'delta'
        }
    }
)

print(f"Delta table '{table_name}' created in Glue Data Catalog under '{database_name}'.")

# Stop the Spark session
spark.stop()

