import duckdb
import pandas as pd
import quixstreams as qx
import datetime as dt

# Create DB (embedded DBMS)
conn = duckdb.connect(database='distance-stats.duckdb')
# create a new table if it doesn't already exist.
conn.execute('''CREATE TABLE IF NOT EXISTS dist_calc
                 (track_id INTEGER PRIMARY KEY, distance REAL)''')

print("Using local kafka")
client = qx.KafkaStreamingClient('127.0.0.1:9092')

topic_consumer = client.get_topic_consumer("distance-calcs", "database-writer", auto_offset_reset=qx.AutoOffsetReset.Earliest)

print(f'Initialized Quix Streams client at {dt.datetime.utcnow()}')

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    # Log the prediction in a human-readable format
    print("Distance calc received: \n", df.to_markdown(), "\n\n")
    print("Updating DB...")
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM dist_calc WHERE track_id = {int(df['track_id'])};")
    existing_record = cur.fetchone()

    # Check if a record with the ID "123" already exists
    if existing_record is None:
        # Create a new record
        print(
            f"Attempting to INSERT INTO dist_calc (track_id, distance) VALUES ({int(df['track_id'])}, {float(df['distance'])});")
        cur.execute(
            f"INSERT INTO dist_calc (track_id, distance) VALUES ({int(df['track_id'])}, {float(df['distance'])});")
    else:
        # Update the existing record
        print(
            f"Attempting to UPDATE dist_calc SET distance = {float(df['distance'])} WHERE track_id = {int(df['track_id'])}")
        cur.execute(
            f"UPDATE dist_calc SET distance = {float(df['distance'])} WHERE track_id = {int(df['track_id'])}")
    conn.commit()

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Subscribe to new DataFrames being received
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()