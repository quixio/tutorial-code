import quixstreams as qx
import os
import pandas as pd
import datetime

triggered = False
average = 0
total = 0 
count = 0

client = qx.QuixStreamingClient()

topic_consumer = client.get_topic_consumer(os.environ["pressure_values"], consumer_group = "empty-transformation")
topic_producer = client.get_topic_producer(os.environ["pressure_events"])
topic_averages = client.get_topic_producer(os.environ["pressure_averages"])

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    global triggered, average, count, total
    stream_producer = topic_producer.get_or_create_stream(stream_id = stream_consumer.stream_id)
    stream_averages = topic_averages.get_or_create_stream("pressure_averages")
    pressure = df['Pressure'][0]
    if not triggered:
        if  pressure > 0:
            print('State ON')
            triggered = True
            count = 0
            average = 0
            total = 0
            stream_producer.events \
                .add_timestamp(datetime.datetime.utcnow()) \
                .add_value("PressureState", "ON") \
                .publish()
    else:
        count = count + 1
        total = (total + pressure)
        average = total / count
        if pressure <= 0 :
            print('State OFF')
            triggered = False
            print('average : --> {:.2f}'.format(average))
            stream_averages.timeseries.buffer \
                .add_timestamp(datetime.datetime.utcnow()) \
                .add_value("PressureAverage", float(average)) \
                .publish()
            stream_producer.events \
                .add_timestamp(datetime.datetime.utcnow()) \
                .add_value("PressureState", "OFF") \
                .publish()
            
def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

topic_consumer.on_stream_received = on_stream_received_handler
print("Listening to streams. Press CTRL-C to exit.")
qx.App.run()
