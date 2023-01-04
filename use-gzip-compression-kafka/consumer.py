from kafka import KafkaConsumer
import json

# To consume latest messages from the given topic and auto-commit offsets
consumer = KafkaConsumer('nginx-access-log-compressed-test3',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

for message in consumer:
    # Message value and key are raw bytes -- decode if necessary!
    # For example, for unicode: `message.value.decode('utf-8')`
    m = message.value
    mj = json.loads(m)
    print(f"TOPIC: {message.topic},"
          f"PARTITION: {message.partition},"
          f"OFFSET: {message.offset},"
          f"KEY: {message.key.decode('utf-8')}, "
          f"VALUE: {mj['host']}")