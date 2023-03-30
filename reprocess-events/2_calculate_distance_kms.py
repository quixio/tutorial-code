import quixstreams as qx
from geopy import Point
from geopy.distance import geodesic
import datetime as dt
import pandas as pd

print("Using local kafka")
client = qx.KafkaStreamingClient('127.0.0.1:9092')

df_dict = {}

def calc_distance(df):
    if 'distance_cumulative' not in df:
        cumsum = 0
    else:
        cumsum = df['distance_cumulative'].iloc[0]

    df['point'] = df.apply(lambda row: Point(latitude=row['latitude'], longitude=row['longitude']), axis=1)
    df['point_prev'] = df['point'].shift(1)
    df['distance_kms'] = df.apply(lambda row: geodesic(row['point'], row['point_prev']).kilometers if row['point_prev'] is not None else float('nan'), axis=1)
    df['distance_cumulative'] = df['distance_kms'].cumsum() + cumsum

    return df['distance_cumulative']

# Initialize a Quix Streams consumer to read from the predictions topic (with some extra commit settings)
topic_consumer = client.get_topic_consumer("raw-trackpoints", "distance_calculator", auto_offset_reset=qx.AutoOffsetReset.Earliest)

# Initialize a Quix Streams producer for sending predictions to the predictions topic
print("Initializing producer...")
topic_producer = client.get_topic_producer('distance-calcs')
print(f"Initialized Kafka producer at {dt.datetime.utcnow()}")


def on_dataframe_released_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    global df_dict

    sid = stream_consumer.stream_id
    print("StreamID: ",sid)

    # Check if the key exists in the df_dict dictionary, if not, initialize it with an empty DataFrame
    if sid not in df_dict:
        column_names = ["id","track_id", "distance"]
        df_dict[sid] = pd.DataFrame(columns=column_names)

    # Add last-buffer's last row, to have the previous last location in df
    df = df_dict[sid].append(df)
    df['distance'] = calc_distance(df)

    # Data to output (all rows minus first one, coming from last buffer)
    output_stream = topic_producer.get_or_create_stream(sid)
    print("publishing:\n",df[['id','track_id','distance']].iloc[1:].to_markdown())
    output_stream.timeseries.publish(df.iloc[1:])

    # Update first row for next buffer
    df_dict[sid] = df.iloc[[-1]]

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Create buffer
    buffer = stream_consumer.timeseries.create_buffer()
    buffer.packet_size = 5 # 10 seconds buffer
    buffer.on_dataframe_released = on_dataframe_released_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()
