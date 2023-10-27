import quixstreams as qx
import time
import datetime
import os
from dotenv import load_dotenv

# Obtain client library token from portal

load_dotenv()
token = os.getenv("STREAMING_TOKEN")
client = qx.QuixStreamingClient(token)

# Open a topic to publish data to. Topic is created if it does not exist.
topic_producer = client.get_topic_producer("pressure")

stream = topic_producer.create_stream()
stream.properties.name = "Pressure values"
stream.timeseries.buffer.time_span_in_milliseconds = 100   # Send data in 100 ms chunks

pressure = [0,0,2,5,6,8,10,12,15,20,25,30,35,35,36,36,50,60,70,80,90,40,30,20,5,2,2,0,0,0]
    
def main():
    try:
        n = 0
        while True:
            print('Pressure value: ', pressure[n])
            stream.timeseries \
                .buffer \
                .add_timestamp(datetime.datetime.utcnow()) \
                .add_value("Pressure", pressure[n]) \
                .publish()
            n = n + 1
            if n >= len(pressure):
                n = 0
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing stream")
        stream.close()

if __name__ == '__main__':
    main()
