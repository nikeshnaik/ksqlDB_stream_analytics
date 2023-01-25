import pymongo
from dotenv import load_dotenv
import os
from pathlib import Path
import json
import time
from loggers.log_helper import system_logger

load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI"))
system_logger.info(f"{client.drop_database('streaming_cricket')}")

database = client.streaming_cricket
collection = database.create_collection("twenty_20")

source_data = Path("./application_server/source_data/t20s_jsons")
counter = 0

for file_path in source_data.iterdir():
    if str(file_path).endswith(".json"):
        counter = counter + 1
        # print(json.load(open(file_path)))
        document = json.load(open(file_path))
        collection.insert_one(document) 
        # print(f"Document Inserted : {counter}")
        system_logger.info(f"Document Inserted : {counter}")

        time.sleep(5)


system_logger.info("Dumping of Cricket Json Done.... check events on other side...")










