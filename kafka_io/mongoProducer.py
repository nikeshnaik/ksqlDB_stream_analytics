import os

import pymongo
from dotenv import load_dotenv
from kafka_io.transformations import filterByTournamentEvent
from confluent_kafka import Producer
import json
from pprint import pprint

load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI"))

database = client.streaming_cricket

collection = database.twenty_20

total_events = 0

mongo_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "sasl.mechanisms":"PLAINTEXT",

})

while True:

    print("Waiting for new events from Mongo Atlas....\n")
    event_stream = collection.watch()
    total_events = total_events + 1
    currentEvent = next(event_stream)


    pprint(currentEvent)


    mongo_producer.produce("source",str(total_events), "asdf")
    mongo_producer.poll(10)
    mongo_producer.flush()

    print(f"Total Events polled: {total_events}")
