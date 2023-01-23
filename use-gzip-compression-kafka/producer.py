import json
from kafka import KafkaProducer

# Load the source data from the project folder
file = 'access.log'

producer_compression  = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x:json.dumps(x, default=str).encode('utf-8'),
                        key_serializer=lambda y:str(y).encode("utf-8"),
                        compression_type="gzip",
                        linger_ms=1000,
                        batch_size=10485760)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:json.dumps(x,default=str).encode('utf-8'),
                         key_serializer=lambda y:str(y).encode("utf-8"))


# Add a switch to let us easily switch between methods
compmethod = 'topic'

# Initialize a simple counter for the message key
message_key = 0

# Iterate through file line-by-line
with open(file, "r") as file_handle:

    for line in file_handle:
        print(f"Sending: {line}")
        # Select the relevant compression method based on the switch
        if compmethod == 'topic':
            producer.send(topic="nginx-log-topic-compression", value=line, key=message_key)
        else:
            producer_compression.send(topic="nginx-log-producer-compression", value=line, key=message_key)

        message_key = message_key+1
