import duckdb
import pandas as pd
import quixstreams as qx
import datetime as dt

# Create DB (embedded DBMS)
conn = duckdb.connect(database='distance-stats.duckdb')

# Create a new table if it doesn't already exist.
conn.execute('''CREATE TABLE IF NOT EXISTS dist_calc
                 (track_id INTEGER PRIMARY KEY, distance REAL)''')


# Initialize the Quix Streams client for generic Kafka
print("Using local kafka")
client = qx.KafkaStreamingClient('127.0.0.1:9092')

# Initialize a Quix Streams consumer
topic_consumer = client.get_topic_consumer("distance-calcs", "database-writer", auto_offset_reset=qx.AutoOffsetReset.Earliest)

print(f'Initialized Quix Streams client at {dt.datetime.utcnow()}')

# Process each incoming DataFrame from the "distance-calcs" topic.
def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
  
    # Get the last row of the DataFrame which contains the latest cumulative distance total
    lrow = df.tail(1)
    
    print("Distance calc received: \n", lrow[['track_id','distance']].to_markdown(), "\n\n")
    print("Updating DB...")
    
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM dist_calc WHERE track_id = {int(lrow['track_id'].iloc[0])};")
    existing_record = cur.fetchone()

    # Check if a record with the Device ID already exists
    if existing_record is None:
        # Create a new record
        print(
            f"Attempting to INSERT INTO dist_calc (track_id, distance) VALUES ({int(lrow['track_id'].iloc[0])}, {float(lrow['distance'].iloc[0])});")
        print("(If this console output has stopped, it means you can safely stop this consumer with CTRL-C. You need to stop it in order to query the database.)")
        cur.execute(
            f"INSERT INTO dist_calc (track_id, distance) VALUES ({int(lrow['track_id'].iloc[0])}, {float(lrow['distance'].iloc[0])});")
    else:
        # Update the existing record
        print(
            f"Attempting to UPDATE dist_calc SET distance = {float(lrow['distance'].iloc[0])} WHERE track_id = {int(lrow['track_id'].iloc[0])}")
        print("(If this console output has stopped, it means you can safely stop this consumer with CTRL-C. You need to stop it in order to query the database.)")
        cur.execute(
            f"UPDATE dist_calc SET distance = {float(lrow['distance'].iloc[0])} WHERE track_id = {int(lrow['track_id'].iloc[0])}")
    conn.commit()

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Subscribe to new DataFrames being received
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()
