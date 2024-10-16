import boto3

# Initialize a Glue client
glue = boto3.client('glue', region_name='us-west-2')  # Change to your preferred region

# Define parameters for the new table
database_name = 'your_database_name'
table_name = 'your_table_name'
s3_location = 's3://your-bucket-name/your-data-path/'

# Define the schema for the new table
columns = [
    {'Name': 'id', 'Type': 'int'},
    {'Name': 'name', 'Type': 'string'},
    {'Name': 'age', 'Type': 'int'},
    {'Name': 'email', 'Type': 'string'}
]

# Create the table in Glue Data Catalog
response = glue.create_table(
    DatabaseName=database_name,
    TableInput={
        'Name': table_name,
        'Description': 'A table created from boto3',
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.s3.S3InputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.s3.S3OutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
            },
            'Parameters': {
                'classification': 'json'  # Adjust based on your data format
            }
        },
        'TableType': 'EXTERNAL_TABLE',
    }
)

print(f"Table '{table_name}' created in the Glue Data Catalog under '{database_name}'.")
