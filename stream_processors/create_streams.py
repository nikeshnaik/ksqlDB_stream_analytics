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

    # array_join(extractjsonfield(PAYLOAD, '$.info.date.dates')) as dates,
    # array_join(extractjsonfield(PAYLOAD, '$.info.officials.match_referees')) as referees,
    # array_join(extractjsonfield(PAYLOAD, '$.info.officials.umpires')) as umpires,
    # array_join(extractjsonfield(PAYLOAD, '$.info.teams'), '|') as winner,

    t20_match_stream = """ 
        CREATE STREAM IF NOT EXISTS match_metadata WITH(VALUE_FORMAT='AVRO', KEY_FORMAT='AVRO') as 
        SELECT
                    extractjsonfield(PAYLOAD,  '$.info.match_type_number') as match_type_number, 
                    extractjsonfield(PAYLOAD, '$.info.city') as city,
                    extractjsonfield(PAYLOAD, '$.info.event.name') as event_name,
                    extractjsonfield(PAYLOAD, '$.info.gender') as gender,
                    extractjsonfield(PAYLOAD, '$.info.match_type') as type,
                    extractjsonfield(PAYLOAD, '$.info.outcome.winner') as winner,
                    extractjsonfield(PAYLOAD, '$.info.season') as season,
                    20 as overs, 
                    extractjsonfield(PAYLOAD, '$.info.toss.winner') as toss,
                    extractjsonfield(PAYLOAD, '$.info.venue') as venue
            FROM t20_match_init

        
    """

    read_city_match_number = "SELECT * FROM match_metadata emit changes"

    ksqldb_server_url = "http://localhost:8088"
    client = connect_server(ksqldb_server_url)
    drop_stream(client, "match_metadata")
    drop_stream(client, "t20_match_init")
    print(create_stream(client, t20_match_init_stream))
    print(create_stream(client, t20_match_stream))

    # result = read_stream(client, read_city_match_number)
    # while record := next(result):
    #     print(record)
