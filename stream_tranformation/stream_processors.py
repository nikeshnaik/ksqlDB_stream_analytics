#Todo, read as Kafka COnsumer and do stream processing + dump into duckdb

from stream_tranformation.transformations import isolateMatchMetaData, isolateOverDetails, isolatePlayersDetails
from utils.helpers import DuckDBConn, createMetaDataTable
from confluent_kafka import Consumer,KafkaException, Producer
import json


def getConsumerObject(topic):

    consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "sasl.mechanisms":"PLAINTEXT",
    "group.id": "mongoProduce"
}) 

    consumer.subscribe([topic])

    return consumer

def getProducerObject():


    producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "sasl.mechanisms":"PLAINTEXT",
    })


    return producer


def metadataStreamProcessing(topic):


    consumer = getConsumerObject(topic)
    producer = getProducerObject()

    while True:

        try:

            msg = consumer.poll(timeout=5)
            print(msg)

            if msg is None: continue

            if msg.error():
                raise KafkaException(msg.error())

            msg = json.loads(msg.value())

            metadataRowRecord = isolateMatchMetaData(msg)
            print(metadataRowRecord)
            producer.produce("metadata", json.dumps(metadataRowRecord))
            producer.poll(10)
            producer.flush()


        except KafkaException as k:
            print("Error on Consumer or Producer poll..", str(k))
            consumer.close()
        finally:
            consumer.commit(asynchronous=True)


if __name__ == "__main__":

    topic = "source"
    
    metadataStreamProcessing(topic)

        
    


    



    

