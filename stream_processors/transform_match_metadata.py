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


    t20_match_metadata_stream = """ 

        CREATE STREAM IF NOT EXISTS match_metadata WITH(VALUE_FORMAT='AVRO', KEY_FORMAT='AVRO') as 
        SELECT
                    extractjsonfield(PAYLOAD,  '$.info.match_type_number') as match_type_number, 
                    extractjsonfield(PAYLOAD,  '$.info.balls_per_over') as balls_per_over, 
                    extractjsonfield(PAYLOAD, '$.info.city') as city,
                    extractjsonfield(PAYLOAD, '$.info.event.name') as event_name,
                    extractjsonfield(PAYLOAD, '$.info.gender') as gender,
                    extractjsonfield(PAYLOAD, '$.info.match_type') as type,
                    extractjsonfield(PAYLOAD, '$.info.outcome.winner') as winner,
                    extractjsonfield(PAYLOAD, '$.info.season') as season,
                    extractjsonfield(PAYLOAD, '$.info.dates') as date,
                    extractjsonfield(PAYLOAD, '$.info.officials.match_referees') as referees,
                    extractjsonfield(PAYLOAD, '$.info.officials.umpires') as umpires,
                    extractjsonfield(PAYLOAD, '$.info.officials.tv_umpires') as tv_umpires,
                    extractjsonfield(PAYLOAD, '$.info.teams') as teams,
                    extractjsonfield(PAYLOAD, '$.info.player_of_match') as player_of_match,
                    extractjsonfield(PAYLOAD, '$.info.team_type') as team_type,
                    20 as overs, 
                    CASE  WHEN ARRAY_CONTAINS(JSON_KEYS(extractjsonfield(PAYLOAD, '$.info.outcome.by')), 'runs') THEN 'Runs' WHEN ARRAY_CONTAINS(JSON_KEYS(extractjsonfield(PAYLOAD, '$.info.outcome.by')), 'wickets') THEN 'Wcket' WHEN ARRAY_CONTAINS(JSON_KEYS(extractjsonfield(PAYLOAD, '$.info.outcome')), 'result') AND ARRAY_CONTAINS(JSON_KEYS(extractjsonfield(PAYLOAD, '$.info.outcome')), 'eliminator') then 'Tied & Won by Eliminator: ' + extractjsonfield(PAYLOAD, '$.info.outcome.eliminator') WHEN ARRAY_CONTAINS(JSON_KEYS(extractjsonfield(PAYLOAD, '$.info.outcome')), 'result') THEN extractjsonfield(PAYLOAD, '$.info.outcome.result') ELSE 'null' END AS outcome,
                    extractjsonfield(PAYLOAD, '$.info.toss.winner') as toss,
                    extractjsonfield(PAYLOAD, '$.info.toss.decision') as toss_decision,
                    extractjsonfield(PAYLOAD, '$.info.venue') as venue
            FROM t20_match_init
        
    """

    emit_match_metadata = "SELECT * FROM match_metadata emit changes"

    ksqldb_server_url = "http://localhost:8088"
    client = connect_server(ksqldb_server_url)
    drop_stream(client, "match_metadata")
    print(create_stream(client, t20_match_metadata_stream))

    # result = read_stream(client, emit_match_metadata)
    # while record := next(result):
    #     print(record)
