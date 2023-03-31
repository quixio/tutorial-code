import argparse
import pandas as pd
import time
import datetime as dt
import quixstreams as qx

# Initialize the Quix Streams client for generic Kafka
print("Using local kafka")
client = qx.KafkaStreamingClient('127.0.0.1:9092')

# Initialize the destination topic
print("Initializing topic")
topic_producer = client.get_topic_producer('raw-trackpoints')

print(f'Initialized Quix Streams client at {dt.datetime.utcnow()}')

# Read in the CSV file
df = pd.read_csv("go_track_trackspoints_sm.csv")

# Cycle through all rows in the CSV file and send each row as a message
for i in range(len(df)):
    
    # Create small data frame for each row
    df_r = df.iloc[[i]]
    
    # Fix the time column so it contains unique timestamps (effective UUID for each message)
    df_r["time"] = pd.Timestamp.now()
    
    # Pretty print the message so you can we see what is being sent
    print("Sending Message: \n", df_r.to_markdown())

    # Create separate steams for each device based on "track_id"
    sid = f"device_{df_r['track_id'].iloc[0]}"
    print("StreamID: ", sid)
    
    # Get or create a stream depending on whether it exists already
    output_stream = topic_producer.get_or_create_stream(sid)
    
    # Publish the message to the stream
    output_stream.timeseries.publish(df_r)

    # Optionally wait for a fraction of a second to slow down the stream
    # so that we can see what is happening.
    time.sleep(0.05)
