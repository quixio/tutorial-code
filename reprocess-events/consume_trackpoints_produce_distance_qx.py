import quixstreams as qx
import datetime as dt
from geopy.distance import distance
from collections import deque
import pandas as pd

#1 — Initialize the Quix Streams client (for standalone Kafka)
client = qx.KafkaStreamingClient('127.0.0.1:9092')

#2 — Initialize a Quix Streams consumer to read from the predictions topic (with some extra commit settings)
commit_settings = qx.CommitOptions()
commit_settings.auto_commit_enabled = False
topic_consumer = client.get_topic_consumer("raw-trackpoints", commit_settings=commit_settings,auto_offset_reset=qx.AutoOffsetReset.Earliest)

#3 — Initialize a Quix Streams producer for sending predictions to the predictions topic
print("Initializing producer...")
topic_producer = client.get_topic_producer('distance-calcs')
output_stream = topic_producer.create_stream()

print(f"Initialized Kafka producer at {dt.datetime.utcnow()}")

# Initialize variables for calculating the total distance traveled
total_distance = 0.0
last_location = None
location_buffer = deque(maxlen=10)

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    global total_distance
    global last_location
    global location_buffer
    # Log the prediction in a human-readable format\
    print("Data received: \n", df.to_markdown(), "\n\n")
    # Extract the latitude and longitude coordinates from the message
    latitude = df["latitude"]
    longitude = df["longitude"]
    location = (float(latitude), float(longitude))

    # Add the current location to the buffer
    location_buffer.append(location)

    # Calculate the distance between the current location and the last location, and update the total distance
    if last_location is not None and len(location_buffer) == location_buffer.maxlen:
        distance_traveled = distance(location_buffer[0], location_buffer[-1]).kilometers
        total_distance += distance_traveled

    # Update the last location for the next iteration
    last_location = location_buffer[-1]

    # Create a new DataFrame without the email text to reduce data transmission
    # df_m = df.assign(total_dist=total_distance)[['track_id', 'total_dist']],

    # Publish the spam predictions to the predictions topic
    # output_stream.timeseries.publish(df_m)
    # print("Distance calc sent: \n", df_m.to_markdown(), "\n\n\n")

    data = qx.TimeseriesData()
    data.add_timestamp(dt.datetime.utcnow()) \
        .add_value("distance", float(total_distance)) \
        .add_value("track_id", int(df['track_id']))

    output_stream.timeseries.publish(data)
    # Print the current total distance traveled
    print(f'Sending results: | {dt.datetime.utcnow()} | {total_distance} | {df["track_id"]}  |')


def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Subscribe to new DataFrames being received
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()
