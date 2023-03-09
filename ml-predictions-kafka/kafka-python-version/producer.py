
# importing packages
import pandas as pd
import json
import datetime as dt
from kafka import KafkaProducer

df = pd.read_csv("../spam.csv")

print(df.head())

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x,default=str).encode('utf-8'))

print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))

for i in df.index:

    data = {'MessageID': str(i),
            'MessageBody': df['Message'][i]}

    print(data)
    producer.send(topic="emails", value=data)

print('Sent record to topic at time {}'.format(dt.datetime.utcnow()))
