import quixstreams as qx
import os
import pandas as pd

client = qx.QuixStreamingClient()

topic_consumer = client.get_topic_consumer(os.environ["input"], consumer_group = "empty-transformation")
topic_producer = client.get_topic_producer(os.environ["output"])

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    cpu_load = df['CPU_Load'][0]

    print(f"CPU Load: {cpu_load} %")    
    stream_producer = topic_producer.get_or_create_stream(stream_id = stream_consumer.stream_id)
    if cpu_load > 50: # hard-coded threshold
        print(f"CPU spike of {cpu_load} detected!")
        stream_producer.timeseries.buffer.publish(df)

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

topic_consumer.on_stream_received = on_stream_received_handler
print("Listening to streams. Press CTRL-C to exit.")
qx.App.run()
