from pandas import DataFrame
import time, json
import datetime as dt
import pandas as pd
import tensorflow as tf
from tensorflow import keras
import numpy as np
from kafka import KafkaProducer
from kafka import KafkaConsumer
import tensorflow_text as text

model = keras.models.load_model('model')

# Initialize a Consumer for reading the email messages from the emails topic
consumer = KafkaConsumer(group_id="python-consumer",
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         value_deserializer=lambda x:json.loads(x))

consumer.subscribe("emails")
print('Initialized Kafka consumer at {}'.format(dt.datetime.utcnow()))

# # Initialize a Producer for sending predictions to the predictions topic
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x,default=str).encode('utf-8'))
print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))

for message in consumer:
    # parsing data
    m=message.value
    # Turn the message back into a DataFrame
    df=DataFrame(m,index=[0])
    # Extract the Message from the DataFrame
    dff = pd.Series(df['MessageBody'])
    # Get a spam prediction for the message
    inference = (model.predict(dff) > 0.5).astype(int)

    # Create a new message with the MessageID and the SpamFlag
    data = {'SpamFlag': inference[0][0],
            'MessageID': m['MessageID']}
    # Send the message to the predictions topic.
    print(f'Sending message: {data}')
    producer.send(topic="predictions",value=data)
    print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))
