# Import packages
import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer

# Initialize the Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

# Set a basic message counter and define the file path
counter = 0
file = "online_retail_II.csv"

for chunk in pd.read_csv(file, encoding='unicode_escape', chunksize=10):
    # For each chunk, convert the invoice date into the correct time format
    chunk["InvoiceDate"] = pd.to_datetime(chunk["InvoiceDate"])

    # Set the counter as the message key
    key = str(counter).encode()

    # Convert the data frame chunk into a dictionary
    chunkd = chunk.to_dict()

    # Encode the dictionary into a JSON Byte Array
    data = json.dumps(chunkd, default=str).encode('utf-8')

    # Send the data to Kafka
    producer.send(topic="transactions", key=key, value=data)

    # Sleep to simulate a real-world interval
    sleep(0.5)

    # Increment the message counter for the message key
    counter = counter + 1

    print(f'Sent record to topic at time {dt.datetime.utcnow()}')