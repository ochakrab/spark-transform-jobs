import boto3
import json
from botocore.exceptions import ClientError

# Initialize clients
s3 = boto3.client('s3')
glue = boto3.client('glue', region_name='us-west-2')  # Change to your preferred region

# Parameters
bucket_name = 'your-bucket-name'
json_file_key = 'path/to/config.json'  # S3 path to your JSON configuration file

def load_configuration_from_s3(bucket, key):
    """Load JSON configuration from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    config_content = response['Body'].read().decode('utf-8')
    return json.loads(config_content)

def check_table_exists(database_name, table_name):
    """Check if the specified table exists in Glue Data Catalog."""
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        return True  # Table exists
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False  # Table does not exist
        else:
            raise

def create_parquet_table(database_name, table_name, s3_location, columns):
    """Create a new table in Glue Data Catalog from Parquet files."""
    if not check_table_exists(database_name, table_name):
        glue.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'Description': 'Table created from Parquet files',
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.parquet.hive.DeprecatedParquetInputFormat',
                    'OutputFormat': 'org.apache.parquet.hive.DeprecatedParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    },
                    'Parameters': {
                        'classification': 'parquet'
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
            }
        )
        print(f"Table '{table_name}' created in Glue Data Catalog under '{database_name}'.")
    else:
        print(f"Table '{table_name}' already exists in Glue Data Catalog under '{database_name}'.")

# Load configuration from S3
try:
    config = load_configuration_from_s3(bucket_name, json_file_key)

    # Extract parameters
    database_name = config['database_name']
    table_name = config['table_name']
    s3_location = config['s3_location']
    columns = config['columns']

    # Run the function to create the table from Parquet files
    create_parquet_table(database_name, table_name, s3_location, columns)

except ClientError as e:
    print(f"Error retrieving the configuration file: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
