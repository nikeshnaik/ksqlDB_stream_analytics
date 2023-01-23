#Todo, read as Kafka COnsumer and do stream processing + dump into duckdb

from stream_tranformation.transformations import isolateMatchMetaData, isolateOverDetails, isolatePlayersDetails
from utils.helpers import DuckDBConn
from confluent_kafka import Consumer,KafkaException
import json

db_path = "data_warehouse/olap_streaming_data"
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "sasl.mechanisms":"PLAINTEXT",
    "group.id": "mongoProduce"

})



with DuckDBConn(db_path) as cursor:

    consumer.subscribe(["source"])


    while True:

        msg = consumer.poll(timeout=5)


        if msg is None: continue

        if msg.error():
            raise KafkaException(msg.error())


        msg = json.loads(msg.value().decode("utf-8"))

        metadataTable = isolateMatchMetaData(msg)




        consumer.commit(asynchronous=True)




    consumer.close()




     


