from ksql import KSQLAPI


def connect_server(url):
    return KSQLAPI(url)


def create_stream(client, stream_query):
    result = client.ksql(stream_query)
    return result


def drop_stream(client, stream):
    return client.ksql(f"DROP STREAM IF EXISTS {stream}")


def read_stream(client, query):
    return client.query(query)


if __name__ == "__main__":
    t20_match_init_stream = """

    CREATE STREAM IF NOT EXISTS t20_match_init (
                                schema STRUCT<type STRING,optional boolean>,
                                payload STRING
                                ) with(
                                    KAFKA_TOPIC = 'source_mongo',
                                    VALUE_FORMAT = 'JSON',
                                    KEY_FORMAT = 'JSON'
                                )

    """

    read_city_match_number = "SELECT * FROM match_metadata emit changes"

    ksqldb_server_url = "http://localhost:8088"
    client = connect_server(ksqldb_server_url)
    drop_stream(client, "match_metadata")
    drop_stream(client, "t20_match_init")
    print(create_stream(client, t20_match_init_stream))

    # result = read_stream(client, read_city_match_number)
    # while record := next(result):
    #     print(record)
