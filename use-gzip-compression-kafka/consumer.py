from kafka import KafkaConsumer
import json

# Consume all the messages from the topic but do not mark them as 'read' (enable_auto_commit=False)
# so that we can re-read them as often as we like.
consumer = KafkaConsumer('nginx-access-log-compressed-test3',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

for message in consumer:
    # Message is decoded from bytes with value_deserializer (in line 8)
    m = message.value
    
    # Load decoded message string as JSON
    mj = json.loads(m)
    
    # Print a subset of data points from the log entry
    print(f"TOPIC: {message.topic},"
          f"PARTITION: {message.partition},"
          f"OFFSET: {message.offset},"
          f"KEY: {message.key.decode('utf-8')}, "
          f"VALUE: {mj['host']}")
