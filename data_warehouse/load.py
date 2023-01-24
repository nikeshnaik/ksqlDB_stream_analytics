from utils.helpers import DuckDBConn
from confluent_kafka import Consumer,KafkaException, Producer
import json


def getConsumerObject(topic):

    consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "sasl.mechanisms":"PLAINTEXT",
    "group.id": "t20games"
}) 

    consumer.subscribe([topic])

    return consumer



def metadataDumpToDuckDB(topic, db_path, table_name):
    consumer = getConsumerObject("metadata")

    with DuckDBConn(db_path) as cursor:

        print("Creating MetaData table if not exist...")

        cursor.execute("""CREATE TABLE IF NOT EXISTS metadata( match_type_number INTEGER,
                                        date DATE[],
                                        city VARCHAR,
                                        event_name VARCHAR(256),
                                        gender VARCHAR,
                                        type VARCHAR,
                                        match_referees VARCHAR[],
                                        match_umpires VARCHAR[],
                                        winner VARCHAR,
                                        overs INTEGER,
                                        season VARCHAR,
                                        team VARCHAR,
                                        toss VARCHAR,
                                        venue VARCHAR
                            )""")

        records = cursor.fetchall()
        print("Table: MetaData created: ",records)


        while True:

            try:

                msg = consumer.poll(timeout=1)

                if msg is None: continue

                if msg.error():
                    raise KafkaException(msg.error())

                msg = json.loads(msg.value())

                cursor.execute("INSERT INTO metadata VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",parameters=tuple(msg.values()))

                print("Row inserted:", cursor.fetchall())

            except KafkaException as k:
                print("Error on Consumer poll..", str(k))
                consumer.close()

            except Exception as e:
                print("Error on DuckDB insertion..", str(e))
                consumer.close()

            finally:
                consumer.commit(asynchronous=True)


if __name__ == "__main__":

    topic = "metadata"
    db_path = "data_warehouse/olap_streaming_data"
    table_name = "metadata"
    metadataDumpToDuckDB("metadata", db_path, table_name)
