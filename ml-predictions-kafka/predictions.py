from pandas import DataFrame
import time, json
import datetime as dt
import pandas as pd
import tensorflow as tf
from tensorflow import keras
import numpy as np
import quixstreams as qx
import tensorflow_text as text

model = keras.models.load_model('model')

#1 — Initialize the Quix Streams client (for standalone Kafka)
client = qx.KafkaStreamingClient('127.0.0.1:9092')

#2 — Initialize a Quix Streams consumer to read from the emails topic (with some extra commit settings)
print("Initializing consumer...")
commit_settings = qx.CommitOptions()
commit_settings.auto_commit_enabled = False # Make sure we can read the same messages again (for testing)
topic_consumer = client.get_topic_consumer("temails", commit_settings=commit_settings,auto_offset_reset=qx.AutoOffsetReset.Earliest)

#2 — Initialize a Quix Streams producer for sending predictions to the predictions topic
print("Initializing producer...")
topic_producer = client.get_topic_producer('tspredictions')
output_stream = topic_producer.create_stream()

print(f"Initialized Kafka producer at {dt.datetime.utcnow()}")

# Initialize the Quix stream handling functions for DataFrames
def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print("Processing Message: \n", df.to_markdown(), "\n")

    # Extract the email text from the DataFrame (in a format that the model expects)
    dff = pd.Series(df['Message'])

    # Get a spam prediction for the email text
    inference = (model.predict(dff) > 0.5).astype(int)

    # Create a new message with the MessageID and the SpamFlag
    df["Spamflag"] = inference

    # In Quix Streams, a message always needs a timestamp
    # so we an extra one for the time of the prediction
    df["TimestampInference"] = time.time_ns() // 1_000_000

    # Create a new DataFrame without the email text to reduce data transmission
    df_m = df[['TimestampInference','ID', 'Spamflag']]

    # Publish the spam predictions to the predictions topic
    output_stream.timeseries.publish(df_m)
    print("Prediction sent: \n", df_m.to_markdown(), "\n\n\n")

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Subscribe to new DataFrames being received
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler


# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()