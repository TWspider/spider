import pymssql
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
import time


class TestSql:
    def __init__(self):
        pass

    def test(self):
        import pyodbc
        conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};SERVER=test;DATABASE=test;UID=user;PWD=password')
        print(conn)

if __name__ == '__main__':
    ts = TestSql()
    time_start = time.time()
    # ts.test()
    time_end = time.time()
    # 运行时间
    print(time_end - time_start)
