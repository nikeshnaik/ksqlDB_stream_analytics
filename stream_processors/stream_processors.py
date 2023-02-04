# Todo, read as Kafka COnsumer and do stream processing + dump into duckdb

from stream_tranformation.transformations import (
    isolateMatchMetaData,
    isolateOverDetails,
    isolatePlayersDetails,
)
from utils.helpers import DuckDBConn, createMetaDataTable
from confluent_kafka import Consumer, KafkaException, Producer
import json
from loggers.log_helper import system_logger


def getConsumerObject(topic):
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "sasl.mechanisms": "PLAINTEXT",
            "group.id": "mongoProduce",
        }
    )

    consumer.subscribe([topic])

    return consumer


def getProducerObject():
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "sasl.mechanisms": "PLAINTEXT",
        }
    )

    return producer


def metadataStreamProcessing(topic):
    consumer = getConsumerObject(topic)
    producer = getProducerObject()

    while True:
        try:
            msg = consumer.poll(timeout=5)
            system_logger.info(msg)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            msg = json.loads(msg.value())

            metadataRowRecord = isolateMatchMetaData(msg)
            system_logger.info(metadataRowRecord)
            producer.produce("metadata", json.dumps(metadataRowRecord))

            producer.poll(10)
            producer.flush()

        except KafkaException as k:
            system_logger.error(f"Error on Consumer or Producer poll.. {str(k)}")
            consumer.close()
        finally:
            consumer.commit(asynchronous=True)


def playerDetailsProcessing(topic):
    consumer = getConsumerObject(topic)
    producer = getProducerObject()

    while True:
        try:
            msg = consumer.poll(timeout=5)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            msg = json.loads(msg.value())

            metadataRowRecord = isolatePlayersDetails(msg)
            system_logger.info(metadataRowRecord)
            producer.produce("playerDetails", json.dumps(metadataRowRecord))
            producer.poll(10)
            producer.flush()

        except KafkaException as k:
            system_logger.info(f"Error on Consumer or Producer poll.., {str(k)}")
        finally:
            consumer.commit(asynchronous=True)


if __name__ == "__main__":
    topic = "source"
    metadataStreamProcessing(topic)
