import quixstreams as qx
import pandas as pd
import psycopg2

# Create a connection to the downstream database
conn = psycopg2.connect(
    host='containers-us-west-204.railway.app',
    port=6307,
    dbname='railway',
    user='postgres',
    password='pCnASnXCIXtP35iCOf1g'
)

# Create a cursor object to execute SQL statements
cur = conn.cursor()

#1 — Initialize the Quix Streams client (for standalone Kafka)
client = qx.KafkaStreamingClient('127.0.0.1:9092')

#2 — Initialize a Quix Streams consumer to read from the predictions topic (with some extra commit settings)
commit_settings = qx.CommitOptions()
commit_settings.auto_commit_enabled = False
topic_consumer = client.get_topic_consumer("distance-calcs", commit_settings=commit_settings,auto_offset_reset=qx.AutoOffsetReset.Earliest)

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    # Log the prediction in a human-readable format
    print("Distance calc received: \n", df.to_markdown(), "\n\n")
    print("Updating DB...")
    cur.execute(f"SELECT * FROM dist_calc WHERE tracking_id = {int(df['track_id'])};")
    existing_record = cur.fetchone()

    # Check if a record with the ID "123" already exists
    if existing_record is None:
        # Create a new record
        print(f"Attempting to INSERT INTO dist_calc (tracking_id, distance) VALUES ({int(df['track_id'])}, {float(df['distance'])});")
        cur.execute(f"INSERT INTO dist_calc (tracking_id, distance) VALUES ({int(df['track_id'])}, {float(df['distance'])});")
    else:
        # Update the existing record
        print(f"Attempting to UPDATE dist_calc SET distance = {float(df['distance'])} WHERE tracking_id = {int(df['track_id'])}")
        cur.execute(f"UPDATE dist_calc SET distance = {float(df['distance'])} WHERE tracking_id = {int(df['track_id'])}")
    conn.commit()

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Subscribe to new DataFrames being received
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()