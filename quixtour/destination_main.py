import quixstreams as qx
import os
import pandas as pd
import vonage # add vonage to requirements.txt to pip install it
from dotenv import load_dotenv # add python-dotenv to requirement.txt

load_dotenv()
vonage_key = os.getenv("VONAGE_API_KEY")
vonage_secret = os.getenv("VONAGE_API_SECRET")
to_number = os.getenv("TO_NUMBER")
send_sms_bool = False # Set this to True if you want to actually send an SMS (you'll need a free Vonage account)

client = vonage.Client(key=vonage_key, secret=vonage_secret)
sms = vonage.Sms(client)

def send_sms(message):
    print("Sending SMS message to admin...")
    responseData = sms.send_message(
        {
            "from": "Vonage APIs",
            "to": to_number,
            "text": message,
        }
    )

    if responseData["messages"][0]["status"] == "0":
        print("Message sent successfully. Admin Alerted.")
    else:
        print(f"Message failed with error: {responseData['messages'][0]['error-text']}")
    return

client = qx.QuixStreamingClient()

topic_consumer = client.get_topic_consumer(topic_id_or_name = os.environ["input"],
                                        consumer_group = "empty-destination")

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print('Spike dataframe received!')
    cpu_load = df['CPU_Load'][0]
    msg = f"Warning! CPU spike of {cpu_load} detected."
    if send_sms_bool is True:
        send_sms(msg)

def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

topic_consumer.on_stream_received = on_stream_received_handler
print("Listening to streams. Press CTRL-C to exit.")
qx.App.run()
