from awpy import Demo
from quixstreams import Application
import random
import os
import json

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="demo_producer", auto_create_topics=True)  # create an Application

# define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)

# Create a parser object with the path to your demo file
dem = Demo("./1-7f613672-b937-4544-a8cc-0f12dee5a151-1-1.dem", verbose=True, ticks=True)

def main():
    """
    Read data from the hardcoded dataset and publish it to Kafka
    """

    # create a pre-configured Producer object.
    with app.get_producer() as producer:
        # iterate over the data from the hardcoded dataset
        for index, row in dem.ticks.iterrows():

            row_json_str = row.to_json()  # convert the row to JSON string
            row_json = json.loads(row_json_str)  # parse JSON string to dictionary

            # publish the data to the topic
            producer.produce(
                topic=topic.name,
                key=row_json['steamid'],
                value=row_json_str.encode('utf-8'),
            )

            print(f"Produced {row_json_str}")

        print("All rows published")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")