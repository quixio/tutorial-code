import quixstreams as qx
import os

client = qx.QuixStreamingClient()

topic_consumer = client.get_topic_consumer(os.environ["input"], consumer_group = "empty-transformation")
topic_producer = client.get_topic_producer(os.environ["output"])

def on_event_data_received_handler(stream_consumer: qx.StreamConsumer, data: qx.EventData):
    if data.value == "UP":
        print ("Up event processed")
    if data.value == "DOWN":
        print ("Down event processed")

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.events.on_data_received = on_event_data_received_handler # register the event data callback

topic_consumer.on_stream_received = on_stream_received_handler
print("Listening to streams. Press CTRL-C to exit.")
qx.App.run()
