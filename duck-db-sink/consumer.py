from quixstreams import Application
from sinks.duckdbsink import DuckDBSink
import os
from dotenv import load_dotenv
load_dotenv()

# Define a processing function to stop the Application after processing a certain amount of messages.
TOTAL_PROCESSED = 0
def on_message_processed(topic:str, partition: int, offset: int):
    """
    Stop application and/or print status after processing {X} number of messages.
    """
    global TOTAL_PROCESSED
    TOTAL_PROCESSED += 1
    ### Check if processing limit is exceeded.
    # Note: Jsonl file only contains 1000 records anyway
    # Keeping this check regardless, in case you want to work with a larger dataset.
    if TOTAL_PROCESSED == 1000:
        print("1000 messages processed")
        # app.stop()


app = Application(
    consumer_group="sink_consumer_v1",
    auto_offset_reset="earliest",
    consumer_extra_config={'max.poll.interval.ms': 300000},
    commit_every=250,
    on_message_processed=on_message_processed,

)
topic = app.topic(os.getenv("input", "raw_data"))

# Initialize DuckDBSink
duckdb_sink = DuckDBSink(
    database_path="csgo_data_v1.db",
    table_name="tick_metadata",
    batch_size=50,

    schema={
        "timestamp": "TIMESTAMP",
        "tick": "INTEGER",
        "inventory": "TEXT",
        "accuracy_penalty": "FLOAT",
        "zoom_lvl": "TEXT",  # Assuming this can be null or empty
        "is_bomb_planted": "BOOLEAN",
        "ping": "INTEGER",
        "health": "INTEGER",
        "has_defuser": "BOOLEAN",
        "has_helmet": "BOOLEAN",
        "flash_duration": "INTEGER",
        "last_place_name": "TEXT",
        "which_bomb_zone": "INTEGER",
        "armor_value": "INTEGER",
        "current_equip_value": "INTEGER",
        "team_name": "TEXT",
        "team_clan_name": "TEXT",
        "game_time": "FLOAT",
        "pitch": "FLOAT",
        "yaw": "FLOAT",
        "X": "FLOAT",
        "Y": "FLOAT",
        "Z": "FLOAT",
        "steamid": "TEXT",
        "name": "TEXT",
        "round": "INTEGER"
    }    # Dictionary of column names and their data types
)

# Create a StreamingDataFrame from the topic
sdf = app.dataframe(topic)
sdf = sdf.update(lambda message: print(f"Received message for tick: {message['tick']}, message count {TOTAL_PROCESSED}"))

# Sink data to InfluxDB
sdf.sink(duckdb_sink)

app.run(sdf)

