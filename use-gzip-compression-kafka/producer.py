import json
from kafka import KafkaProducer

file = 'access.log'

producer_compression  = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:json.dumps(x, default=str).encode('utf-8'),
                         key_serializer=lambda y:str(y).encode("utf-8"),
                         compression_type="gzip",
                         linger_ms=1000,
                         batch_size=10485760)

producer_topiccompression = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:json.dumps(x, default=str).encode('utf-8'),
                         key_serializer=lambda y:str(y).encode("utf-8"))


# ginx-access-log-compressed-trial-1
#10000
#10485760

# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x:json.dumps(x, default=str).encode('utf-8'),
#                          key_serializer=lambda y:str(y).encode("utf-8"),
#                          compression_type="gzip",
#                          linger_ms=0,
#                          batch_size=16384)

compmethod = 'topic'
message_key = 0
with open(file, "r") as file_handle:
    for line in file_handle:
        print(f"Sending: {line}")
        if compmethod == 'topic':
            producer_topiccompression.send(topic="nginx-log-topic-compression", value=line, key=message_key)
        else:
            producer_compression.send(topic="nginx-log-producer-compression ", value=line, key=message_key)
        message_key = message_key+1
