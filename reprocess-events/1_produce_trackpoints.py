import argparse
import pandas as pd
import time
import datetime as dt
import quixstreams as qx

print("Using local kafka")
client = qx.KafkaStreamingClient('127.0.0.1:9092')

# Initialize the destination topic
print("Initializing topic")
topic_producer = client.get_topic_producer('raw-trackpoints')
output_stream = topic_producer.create_stream()
output_stream.properties.name = "Trackpoints from CSV"

print(f'Initialized Quix Streams client at {dt.datetime.utcnow()}')

# Read in the CSV file
df = pd.read_csv("go_track_trackspoints_sm.csv")

for i in range(len(df)):
    # Create small data frame for each message
    df_r = df.iloc[[i]]

    # Print the message so you can see what is being sent
    print("Sending Message: \n", df_r.to_markdown())

    # Send the data with the Quix Streams client
    output_stream.timeseries.publish(df_r)

    # Optionally wait for a fraction of a second to slow down the stream
    # so that we can see what is happening.
    time.sleep(0.05)
