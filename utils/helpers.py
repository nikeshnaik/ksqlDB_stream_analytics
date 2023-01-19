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