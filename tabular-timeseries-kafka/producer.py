# Import packages
import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer

file = r"C:\Users\booki\Documents\_DATASETS\online_retail_II.csv"

# Initialize Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))

# Set a basic counter as the message key
counter = 0

for chunk in pd.read_csv(file,encoding='latin1',chunksize=10):
    # For each chunk, convert the invoice date into the correct time format
    chunk["InvoiceDate"] = pd.to_datetime(chunk["InvoiceDate"])
    #chunk.set_index('InvoiceDate', inplace=True)

    # Set the counter as the message key
    key = str(counter).encode()

    # Convert the data frame chunk into a dictionary including the index
    chunkd = chunk.to_dict()
    # Encode the dictionary into a JSON Byte Array
    data = json.dumps(chunkd, default=str).encode('utf-8')

    # Send the data to Kafka
    producer.send(topic="transactions", key=key, value=data)
    counter = counter + 1

    # Sleep to simulate a real time interval
    sleep(0.5)
    print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))