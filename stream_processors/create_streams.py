from ksql import KSQLAPI


def connect_server(url):
    return KSQLAPI(url)


def create_stream(client, stream_query):
    result = client.ksql(stream_query)

    return result


def drop_stream(client, stream):
    return client.ksql(f"DROP STREAM {stream}")


def read_stream(client, query):
    return client.query(query)


if __name__ == "__main__":
    ksqldb_server_url = "http://localhost:8088"

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

    t20_match_stream = """ 
        CREATE STREAM IF NOT EXISTS city_match_number as 
            SELECT  extractjsonfield(PAYLOAD,  '$.city') as city, 
                    extractjsonfield(PAYLOAD, '$.match_type_number') as match_type_number 
            FROM t20_match_init
    """

    read_city_match_number = "SELECT * FROM city_match_number emit changes"

    client = connect_server(ksqldb_server_url)
    drop_stream(client, "city_match_number")
    drop_stream(client, "t20_match_init")
    print(create_stream(client, t20_match_init_stream))
    print(create_stream(client, t20_match_stream))

    result = read_stream(client, read_city_match_number)
    while record := next(result):
        print(record)
