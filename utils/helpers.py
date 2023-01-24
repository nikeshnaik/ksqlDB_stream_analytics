import duckdb

class DuckDBConn:

    def __init__(self, db_path):

        self.conn = duckdb.connect(database=db_path, read_only=False)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.cursor.commit()
        self.conn.close()


def createMetaDataTable(db_path):

    records = []

    with DuckDBConn(db_path) as cursor:

        cursor.execute("""CREATE TABLE IF NOT EXIST 
                        metadata_match(
                            match_type_number INTEGER, 
                            date DATA,
                            city VARCHAR,
                            event_name VARCHAR,
                            gender VARCHAR,
                            type VARCHAR,
                            match_referee_1 VARCHAR,
                            match_referee_2 VARCHAR,
                            match_umpire_1 VARCHAR,
                            match_umpire_2 VARCHAR,
                            winner VARCHAR,
                            overs INTEGER,
                            season VARCHAR,
                            team VARCHAR,
                            toss VARCHAR,
                            venue VARCHAR
                            )""")

        records = cursor.fetchall()

    return records


def selectAllSample(table_name, db_path):

    with DuckDBConn(db_path) as cursor:

        cursor.execute(f"SELECT * FROM {table_name} limit 20")

        records = cursor.fetchall()

    return records

    

