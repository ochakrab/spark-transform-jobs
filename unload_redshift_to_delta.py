import boto3
from pyspark.sql import SparkSession

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("UnloadRedshiftToS3") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Redshift and S3 configuration
redshift_jdbc_url = "jdbc:redshift://<your-redshift-cluster>:<port>/<database>"
username = "<your-username>"
password = "<your-password>"
redshift_table = "<your-redshift-table>"
s3_bucket = "<your-bucket>"
s3_path = f"s3://{s3_bucket}/your-path/"

# Create a Boto3 client to interact with AWS services
boto3_client = boto3.client('redshift-data', region_name='<your-region>')

# Unload data from Redshift to S3 in Parquet format
unload_query = f"""
UNLOAD ('SELECT * FROM {redshift_table}') 
TO '{s3_path}' 
CREDENTIALS 'aws_access_key_id=<your-access-key>;aws_secret_access_key=<your-secret-key>' 
FORMAT AS PARQUET 
ALLOWOVERWRITE 
PARALLEL OFF;
"""

# Execute the UNLOAD command
response = boto3_client.execute_statement(
    ClusterIdentifier='<your-cluster-id>',
    Database='<database>',
    DbUser=username,
    Sql=unload_query
)

# Check if the UNLOAD was successful
print("Unload response:", response)

# Read the unloaded data from S3 into a Spark DataFrame
df = spark.read.parquet(s3_path)

# Write the DataFrame back to S3 in Delta format with partitioning
delta_path = f"s3://{s3_bucket}/your-delta-path/"
df.write \
    .format("delta") \
    .mode("overwrite") \  # Use "append" if you want to add to existing data
    .partitionBy("partition_column") \  # Replace with your partition column(s)
    .save(delta_path)

# Stop the Spark session
spark.stop()
