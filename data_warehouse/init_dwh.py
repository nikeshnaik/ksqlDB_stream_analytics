import duckdb
from utils.helpers import DuckDBConn

data_warehouse = "./data_warehouse/olap_streaming_data"

with DuckDBConn(data_warehouse) as cursor:

    print(cursor.execute('select 42').fetchall())




