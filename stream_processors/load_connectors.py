from ksql import KSQLAPI
from stream_processors.connectors import (
    mongo_source_connector_config,
    jdbc_sink_duckdb_connector_config,
)
from pprint import pprint
import time


def connect_server(url):
    return KSQLAPI(url=url)


def check_if_connectors_exist(client, connector_name):
    result = client.ksql("show connectors")
    connectors = result[0]["connectors"]
    print(connectors, connector_name)
    for each in connectors:
        if each["name"].lower() == connector_name:
            return True
    return False


def drop_connector(client, name):
    return client.ksql(f"DROP CONNECTOR {connector_name}")


def create_connectors(client, config):
    return client.ksql(config)


if __name__ == "__main__":
    ksqldb_server_url = "http://localhost:8088"

    connectors_with_config = {
        "mongo_source_cricket": mongo_source_connector_config,
        "duckdb_sink_cricket": jdbc_sink_duckdb_connector_config,
    }

    client = connect_server(ksqldb_server_url)

    for connector_name, connector_config in connectors_with_config.items():
        print(connector_name)
        if not check_if_connectors_exist(client, connector_name):
            result = create_connectors(client, connector_config)
            pprint(f"Connectors Created: {connector_name}")
        else:
            result = drop_connector(client, connector_name)
            pprint(f"Connector dropped: {connector_name}")
            result = create_connectors(client, connector_config)
            pprint(f"Connector created: {connector_name}")
