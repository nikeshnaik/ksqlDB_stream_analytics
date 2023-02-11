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


    t20_match_innings_stream = """ 

        CREATE STREAM IF NOT EXISTS match_innings WITH(VALUE_FORMAT='AVRO', KEY_FORMAT='AVRO') as 
            SELECT JSON_RECORDS(EXTRACTJSONFIELD(PAYLOAD, '$.info.players')) as players, 
                EXTRACTJSONFIELD(PAYLOAD, '$.info.teams') as teams, JSON_RECORDS(EXTRACTJSONFIELD(PAYLOAD, '$.info.registry.people')), 
                EXTRACTJSONFIELD(PAYLOAD, '$.info.match_type_number') as match_type_number,  
                EXTRACTJSONFIELD(PAYLOAD, '$.info.match_type') as type 
            FROM t20_match_init
    """

    emit_match_metadata = "SELECT * FROM match_players emit changes"

    ksqldb_server_url = "http://localhost:8088"
    client = connect_server(ksqldb_server_url)
    drop_stream(client, "match_players")
    print(create_stream(client, t20_match_innings_stream))

    result = read_stream(client, emit_match_metadata)
    while record := next(result):
        print(record)
