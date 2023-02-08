from utils.helpers import DuckDBConn
from confluent_kafka import Consumer, KafkaException, Producer
import json


def metadataDumpToDuckDB(topic, db_path, table_name):
    with DuckDBConn(db_path) as cursor:
        print("Creating MetaData table ...")
        # match_referees VARCHAR[],
        # match_umpires VARCHAR[],
        # team VARCHAR,
        # date DATE[],
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS metadata( match_type_number INTEGER,
                                        city VARCHAR,
                                        event_name VARCHAR(256),
                                        gender VARCHAR,
                                        type VARCHAR,
                                        winner VARCHAR,
                                        overs INTEGER,
                                        season VARCHAR,
                                        toss VARCHAR,
                                        venue VARCHAR
                            )"""
        )


if __name__ == "__main__":
    topic = "metadata"
    db_path = "data_warehouse/olap_streaming_data"
    table_name = "metadata"
    metadataDumpToDuckDB("metadata", db_path, table_name)
