import quixstreams as qx
from transformers import Pipeline
import pandas as pd
import re

with open('banned_words.txt', 'r', encoding='ISO-8859-1') as f:
    lines = f.readlines()[9:]  # Skip the first 9 lines
    banned_words = ', '.join(lines).split(', ')

def censor_banned_words(text, banned_words):
    for word in banned_words:
        text = re.sub(r'\b' + re.escape(word) + r'\b', '*' * len(word), text, flags=re.IGNORECASE)
    return text

class QuixFunction:
    def __init__(self, consumer_stream: qx.StreamConsumer, producer_stream: qx.StreamProducer, producer_stream_sanitized: qx.StreamProducer, classifier: Pipeline):
        self.consumer_stream = consumer_stream
        self.producer_stream = producer_stream
        self.producer_stream_sanitized = producer_stream_sanitized  # New
        self.classifier = classifier
        self.sum = 0
        self.count = 0

    # Callback triggered for each new parameter data.
    def on_dataframe_handler(self, consumer_stream: qx.StreamConsumer, df_all_messages: pd.DataFrame):

        # Sanitize text for message history
        df_sanitized = df_all_messages.copy()
        df_sanitized["chat-message"] = df_sanitized["chat-message"].apply(
            lambda x: censor_banned_words(x, banned_words))

        # Output sanitized data
        print(f'sending sanitized: {df_sanitized["chat-message"][0]}')
        self.producer_stream_sanitized.timeseries.publish(df_sanitized)

        # Use the model to predict sentiment label and confidence score on received messages
        model_response = self.classifier(list(df_all_messages["chat-message"]))

        # Add the model response ("label" and "score") to the pandas dataframe
        df = pd.concat([df_all_messages, pd.DataFrame(model_response)], axis = 1)

        # Iterate over the df to work on each message
        for i, row in df.iterrows():

            # Calculate "sentiment" feature using label for sign and score for magnitude
            df.loc[i, "sentiment"] = row["score"] if row["label"] == "POSITIVE" else - row["score"]

            # Add average sentiment (and update memory)
            self.count = self.count + 1
            self.sum = self.sum + df.loc[i, "sentiment"]
            df.loc[i, "average_sentiment"] = self.sum/self.count

            # Output data with new features
            print(f'sending original message: {df["chat-message"][0]}')
            self.producer_stream.timeseries.publish(df)
