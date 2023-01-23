from kafka import KafkaConsumer
import json
import pandas as pd

# Consume all the messages from the topic but do not mark them as 'read' (enable_auto_commit=False)
# so that we can re-read them as often as we like.
consumer = KafkaConsumer('transactions',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

for message in consumer:
    mframe = pd.DataFrame(message.value)

    # Multiply the quantity by the price and store in a new "revenue" column
    mframe['revenue'] = mframe['Quantity'] * mframe['Price']

    # Aggregate the StockCodes in the individual batch by revenue
    summary = mframe.groupby('StockCode')['revenue'].sum()

    print(summary)


