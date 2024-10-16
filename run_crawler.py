import boto3
import time

# Initialize a Glue client
glue = boto3.client('glue', region_name='us-west-2')  # Change to your preferred region

# Specify the name of the crawler you want to run
crawler_name = 'your_crawler_name'

def start_crawler(crawler_name):
    # Start the crawler
    response = glue.start_crawler(Name=crawler_name)
    print(f"Crawler '{crawler_name}' started.")

    # Optionally, wait for the crawler to finish
    while True:
        response = glue.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']
        print(f"Crawler '{crawler_name}' is currently in state: {state}")
        
        if state in ['READY', 'STOPPING', 'STOPPED']:
            print("Crawler has completed its run.")
            break

        time.sleep(5)  # Wait for a few seconds before checking again

# Run the crawler
start_crawler(crawler_name)
