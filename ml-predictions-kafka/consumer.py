import quixstreams as qx
import pandas as pd

#1 — Initialize the Quix Streams client (for standalone Kafka)
client = qx.KafkaStreamingClient('127.0.0.1:9092')

#2 — Initialize a Quix Streams consumer to read from the predictions topic (with some extra commit settings)
commit_settings = qx.CommitOptions()
commit_settings.auto_commit_enabled = False
topic_consumer = client.get_topic_consumer("tspredictions", commit_settings=commit_settings,auto_offset_reset=qx.AutoOffsetReset.Earliest)
def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    # Log the prediction in a human-readable format
    print("Prediction received: \n", df.to_markdown(), "\n\n")

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # Subscribe to new DataFrames being received
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

# Subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()