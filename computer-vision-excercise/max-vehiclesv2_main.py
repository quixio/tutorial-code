import quixstreams as qx
import os
import pandas as pd
from datetime import datetime, timedelta

client = qx.QuixStreamingClient()

topic_consumer = client.get_topic_consumer(os.environ["input"], consumer_group="max-vehicles-v2",
                                           auto_offset_reset=qx.AutoOffsetReset.Earliest)
topic_producer = client.get_topic_producer(os.environ["output"])
topic_producer2 = client.get_topic_producer(os.environ["output2"])


pd.set_option('display.max_columns', None)


# Define a function to calculate the maximum vehicles seen over the last 24 hours
def calculate_max_vehicles(stream_data):
    max_vehicles = 0

    # calculate the start of the window
    start_time = datetime.utcnow() - timedelta(hours=24)

    # remove any entries with a date more than 24 hours in the past
    filtered = []
    for item in stream_data["count"]:
        if item[0] > start_time:
            filtered.append(item)

    # store the newly cleaned data in state
    stream_data["count"] = filtered

    # determine the max vehicles in the last 24 hours
    for _, vehicles in stream_data["count"]:
        max_vehicles = max(max_vehicles, vehicles)

    return max_vehicles

def calculate_max_count(stream_data, count_type):
    max_count = 0
    start_time = datetime.utcnow() - timedelta(hours=24)
    filtered = []
    for item in stream_data[count_type]:
        if item[0] >= start_time:
            filtered.append(item)
            max_count = max(max_count, item[1])
    stream_data[count_type] = filtered
    return max_count

# Declare dictionaries as global variables
max_vehicles_per_camera = {}
max_buses_per_camera = {}
max_cars_per_camera = {}
max_motorbikes_per_camera = {}
max_trucks_per_camera = {}

def process_max_window_data(stream_consumer, new_data_frame):
    global max_vehicles_per_camera, max_buses_per_camera, max_cars_per_camera, max_motorbikes_per_camera, max_trucks_per_camera
    stream_id = stream_consumer.stream_id
    stream_data = stream_consumer.get_dict_state("data", lambda missing_key: [])
    last_max = stream_consumer.get_scalar_state("last_max", lambda: 0)

    for i, dataframe in new_data_frame.iterrows():
        timestamp = datetime.utcfromtimestamp(dataframe["timestamp"] / 1e9)  # Convert timestamp to datetime
        num_vehicles = dataframe["vehicles"]
        num_buses = dataframe.get("bus", 0)
        num_cars = dataframe.get("car", 0)
        num_trucks = dataframe.get("truck", 0)
        num_motorbikes = dataframe.get("motorcycle", 0)

        stream_data["count_vehicles"].append((timestamp, num_vehicles))
        stream_data["count_buses"].append((timestamp, num_buses))
        stream_data["count_cars"].append((timestamp, num_cars))
        stream_data["count_trucks"].append((timestamp, num_trucks))
        stream_data["count_motorbikes"].append((timestamp, num_motorbikes))

        max_vehicles = calculate_max_count(stream_data, "count_vehicles")
        max_buses = calculate_max_count(stream_data, "count_buses")
        max_cars = calculate_max_count(stream_data, "count_cars")
        max_trucks = calculate_max_count(stream_data, "count_trucks")
        max_motorbikes = calculate_max_count(stream_data, "count_motorbikes")

        print(f"Stream {stream_id}: Maximum vehicles seen in the last 24 hours: {max_vehicles}")
        print(f"Stream {stream_id}: Maximum buses seen in the last 24 hours: {max_buses}")
        print(f"Stream {stream_id}: Maximum cars seen in the last 24 hours: {max_cars}")
        print(f"Stream {stream_id}: Maximum trucks seen in the last 24 hours: {max_trucks}")
        print(f"Stream {stream_id}: Maximum motorbikes seen in the last 24 hours: {max_motorbikes}")

        max_vehicles_per_camera[stream_id] = max(max_vehicles_per_camera.get(stream_id, 0), max_vehicles)
        max_buses_per_camera[stream_id] = max(max_buses_per_camera.get(stream_id, 0), max_buses)
        max_cars_per_camera[stream_id] = max(max_cars_per_camera.get(stream_id, 0), max_cars)
        max_trucks_per_camera[stream_id] = max(max_trucks_per_camera.get(stream_id, 0), max_trucks)
        max_motorbikes_per_camera[stream_id] = max(max_motorbikes_per_camera.get(stream_id, 0), max_motorbikes)

        data = {'timestamp': datetime.utcnow(),
                'max_vehicles': [max_vehicles],
                'TAG__cam': stream_id}
        df2 = pd.DataFrame(data)

        if max_vehicles != last_max.value:
            last_max.value = max_vehicles
            stream_producer = topic_producer.get_or_create_stream(stream_id=stream_id)
            stream_producer.timeseries.buffer.publish(df2)

        total_max_buses = sum(max_buses_per_camera.values())
        total_max_cars = sum(max_cars_per_camera.values())
        total_max_trucks = sum(max_trucks_per_camera.values())
        total_max_motorbikes = sum(max_motorbikes_per_camera.values())
        total_max_vehicles = total_max_buses + total_max_cars + total_max_trucks + total_max_motorbikes
        
        # Print the total observed for each vehicle type
        print(f" -- Total maximum vehicles observed for all cameras: {total_max_vehicles}")
        print(f" -- Total maximum buses observed for all cameras: {total_max_buses}")
        print(f" -- Total maximum cars observed for all cameras: {total_max_cars}")
        print(f" -- Total maximum trucks observed for all cameras: {total_max_trucks}")
        print(f" -- Total maximum motorbikes observed for all cameras: {total_max_motorbikes}")

        agg_data = {'timestamp': [datetime.utcnow()],
                    'combined_max_vehicles_for_all_cameras': [total_max_vehicles],
                    'combined_max_buses_for_all_cameras': [total_max_buses],
                    'combined_max_cars_for_all_cameras': [total_max_cars],
                    'combined_max_trucks_for_all_cameras': [total_max_trucks],
                    'combined_max_motorbikes_for_all_cameras': [total_max_motorbikes]}
        df_agg = pd.DataFrame(agg_data)

        stream_producer2 = topic_producer2.get_or_create_stream(stream_id="aggregated_data")
        stream_producer2.timeseries.buffer.publish(df_agg)


def on_stream_received_handler(outer_stream_consumer: qx.StreamConsumer):
    def on_dataframe_received_handler(inner_stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
        #print(inner_stream_consumer.stream_id)
        process_max_window_data(inner_stream_consumer, df)

    outer_stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler


# subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

def before_shutdown():
    topic_producer.flush()
    topic_producer.dispose()
    topic_consumer.dispose()


# Handle termination signals and provide a graceful exit
qx.App.run(before_shutdown=before_shutdown)
