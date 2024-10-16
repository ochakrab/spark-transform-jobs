import boto3
from botocore.exceptions import ClientError

# Initialize a Glue client
glue = boto3.client('glue', region_name='us-west-2')  # Change to your preferred region

# Define parameters for the new table
database_name = 'your_database_name'
table_name = 'your_delta_table_name'
s3_location = 's3://your-bucket-name/delta-table/'

def check_table_exists(database_name, table_name):
    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
        return True  # Table exists
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False  # Table does not exist
        else:
            raise

def create_delta_table():
    if not check_table_exists(database_name, table_name):
        # Define the schema for the new table
        columns = [
            {'Name': 'id', 'Type': 'int'},
            {'Name': 'name', 'Type': 'string'},
            {'Name': 'age', 'Type': 'int'},
            {'Name': 'salary', 'Type': 'float'},
            {'Name': 'hire_date', 'Type': 'date'}
        ]
        
        # Create the table in Glue Data Catalog
        glue.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'Description': 'Delta table created if not exists',
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.HiveDeltaInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveDeltaOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.spark.sql.delta.serialization.DeltaSerDe'
                    },
                    'Parameters': {
                        'classification': 'delta'
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
            }
        )
        print(f"Delta table '{table_name}' created in Glue Data Catalog under '{database_name}'.")
    else:
        print(f"Table '{table_name}' already exists in Glue Data Catalog under '{database_name}'.")

# Run the function to create the Delta table
create_delta_table()
