import os

import pymongo
from dotenv import load_dotenv
from confluent_kafka import Producer
import json
from pprint import pprint
from loggers.log_helper import system_logger


load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI"))

database = client.streaming_cricket

collection = database.twenty_20

total_events = 0

mongo_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "sasl.mechanisms":"PLAINTEXT",
    "group.id": "mongoProduce"

})

while True:

    system_logger.info("Waiting for new events from Mongo Atlas....\n")
    event_stream = collection.watch()
    total_events +=1

    currentEvent = next(event_stream)
    currentEvent = currentEvent.get("fullDocument", {})
    if "_id" in currentEvent:
        del currentEvent["_id"]

    if currentEvent:
        currentEvent = json.dumps(currentEvent)
        mongo_producer.produce("source", currentEvent)

    mongo_producer.poll(10)
    mongo_producer.flush()


    system_logger.info(f"Total Events flushed: {total_events}")
