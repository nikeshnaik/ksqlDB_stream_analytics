import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI"))

database = client.streaming_cricket

collection = database.twenty_20

total_events = 0

while True:

    print("Waiting for new events from Mongo Atlas....\n")
    event = collection.watch()
    total_events = total_events + 1
    print(next(event))
    print(f"Total Events polled: {total_events}")
