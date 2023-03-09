# importing packages
import pandas as pd
import time
import datetime as dt
import quixstreams as qx

# Initialize the Quix Streams client
client = qx.KafkaStreamingClient('127.0.0.1:9092')

# Initialize the destination topic
print("Initializing topic")
topic_producer = client.get_topic_producer('temails')
output_stream = topic_producer.create_stream()

print(f'Initialized Quix Streams client at {dt.datetime.utcnow()}')

# Read in the CSV file
df = pd.read_csv("spam-timeseries.csv")

for i in range(len(df)):
    # Create small data frame for each message
    df_r = df.iloc[[i]]

    # Print the message so you can see what is being sent
    print("Sending Message: \n", df_r.to_markdown())

    # Send the data with the Quix Streams client
    output_stream.timeseries.publish(df_r)

    # Optionally wait for half a second to slow down the stream
    # so that we can see what is happening.
    time.sleep(0.5)