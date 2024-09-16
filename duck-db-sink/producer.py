from quixstreams import Application  # import the Quix Streams modules for interacting with Kafka
import os
import json

from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="data_source", auto_create_topics=True)  # create an Application

# define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)

# define the path to the JSONL file
jsonl_file_path = os.environ["jsonl_file"]

def get_data():
    """
    A function to load data from a JSON Lines file.
    It reads each line of the file and returns a list of JSON objects.
    """
    data_with_id = []
    
    with open(jsonl_file_path, 'r') as f:
        for line in f:
            row_data = json.loads(line)  # parse each line as a JSON object
            data_with_id.append(row_data)
    
    return data_with_id


def main():
    """
    Read data from the JSON Lines file and publish it to Kafka
    """
    # create a pre-configured Producer object.
    with app.get_producer() as producer:
        # iterate over the data from the JSON Lines file
        data_with_id = get_data()
        for row_data in data_with_id:

            json_data = json.dumps(row_data)  # convert the row to JSON

            # publish the data to the topic
            producer.produce(
                topic=topic.name,
                key=row_data['steamid'],
                value=json_data,
            )

        print("All rows published")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
