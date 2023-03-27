import quixstreams as qx
from geopy import Point
from geopy.distance import geodesic
import datetime as dt
import pandas as pd

print("Using local kafka")
client = qx.KafkaStreamingClient('127.0.0.1:9092')

df_i = pd.DataFrame()

def calc_distance(df):
    df['point'] = df.apply(lambda row: Point(latitude=row['latitude'], longitude=row['longitude']), axis=1)
    df['point_prev'] = df['point'].shift(1)
    df['distance_miles'] = df.apply(lambda row: geodesic(row['point'], row['point_prev']).miles if row['point_prev'] is not None else float('nan'), axis=1)
    return df['distance_miles']

#2 — Initialize a Quix Streams consumer to read from the predictions topic (with some extra commit settings)
topic_consumer = client.get_topic_consumer("raw-trackpoints", "distance_calculator", auto_offset_reset=qx.AutoOffsetReset.Earliest)

#3 — Initialize a Quix Streams producer for sending predictions to the predictions topic
print("Initializing producer...")
topic_producer = client.get_topic_producer('distance-calcs')
output_stream = topic_producer.create_stream()

print(f"Initialized Kafka producer at {dt.datetime.utcnow()}")


def on_dataframe_released_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):

    global df_i
    # Add last-buffer's last row, to have the previous last location in df
    df = pd.concat([df_i, df], ignore_index=True)

    # Add distance
    df['distance'] = calc_distance(df)
    latest = df.tail(1)

    # Data to output (all rows minus first one, coming from last buffer)
    output_stream.timeseries.publish(df.iloc[1:])
    # Print the current total distance traveled
    print(f"Sending results: | timestamp: [{dt.datetime.utcnow()}] | total distance so far: [{latest['distance'].iloc[0]}.] | device id: [{latest['track_id'].iloc[0]}]  |")

    # Update first row for next buffer
    df_i = df.iloc[[-1]]

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Create buffer
    buffer = stream_consumer.timeseries.create_buffer()
    buffer.time_span_in_milliseconds = 10000 # 10 seconds buffer
    buffer.on_dataframe_released = on_dataframe_released_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()