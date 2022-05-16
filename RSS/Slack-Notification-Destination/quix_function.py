from quixstreaming import StreamReader, ParameterData, EventData
import requests
import pandas as pd

class QuixFunction:
    def __init__(self, webhook_url, input_stream: StreamReader):
        self.webhook_url = webhook_url
        self.input_stream = input_stream

        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)

    # Callback triggered for each new parameter data.
    def on_parameter_data_handler(self, data: ParameterData):

        # send your slack message
        # gather the component parts of the message
        
        title = data.timestamps[0].parameters["title"].string_value
        tagmatch = data.timestamps[0].parameters["TAG_MATCH"].string_value
        url = data.timestamps[0].parameters["id"].string_value

        # build the message
        slack_message = {"text": "HEY! A new post is up on StackOverflow!\n{} \nmatched tag '{}'\nsee more here {}".format(title, tagmatch, url)}
        
        # send the message
        requests.post(self.webhook_url, json=slack_message)

    # Callback triggered for each new event.
    def on_event_data_handler(self, data: EventData):
        # we aren't using events for this
        pass