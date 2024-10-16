from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RedshiftDeleteJob") \
    .getOrCreate()

# Load your Spark DataFrame (replace with your actual data source)
spark_df = spark.read.csv("s3://your-bucket/your-file.csv", header=True)

# Assuming you have the Redshift JDBC URL, username, and password
redshift_jdbc_url = "jdbc:redshift://<your-redshift-cluster>:<port>/<database>"
username = "<your-username>"
password = "<your-password>"
staging_table = "your_staging_table"
main_table = "your_main_table"

# Write the Spark DataFrame to the staging table in Redshift
spark_df.write \
    .format("jdbc") \
    .option("url", redshift_jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \  # or "append" if you want to keep existing data
    .save()

# Create SQL to delete records from the main table based on the join with the staging table
delete_sql = f"""
DELETE FROM {main_table} 
WHERE id IN (
    SELECT m.id
    FROM {main_table} m
    JOIN {staging_table} s ON m.id = s.id  -- Replace 'id' with your join column(s)
)
"""

# Connect to Redshift using SQLAlchemy and execute the delete
engine = create_engine(f'redshift+psycopg2://{username}:{password}@<your-redshift-cluster>:<port>/<database>')
with engine.connect() as connection:
    connection.execute(delete_sql)

# Optionally, you can drop the staging table after the delete operation
with engine.connect() as connection:
    connection.execute(f"DROP TABLE IF EXISTS {staging_table}")

# Stop the Spark session
spark.stop()
