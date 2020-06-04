import pymssql
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
import time
import pandas as pd


class TestSql:
    def __init__(self):
        pass

    def test(self):
        from sqlalchemy import create_engine
        host = '10.10.202.13'
        user = 'bigdata_user'
        password = 'ulyhx3rxqhtw'
        database = 'TWSpider'
        self.engine_third_house = create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
        sql = "select DISTINCT RoomId from ThirdHouseRankAnjuke"
        res = pd.read_sql(sql, con=self.engine_third_house)["ThirdId"].astype('str').tolist()
        res = ','.join(res)
        print(res)
        sql = "select DISTINCT RoomId from ThirdHouseRankAnjuke_old where RoomId not in ({ls})".format(ls=res)
        print(sql)
        res = pd.read_sql(sql, con=self.engine_third_house)
        print(res)


if __name__ == '__main__':
    ts = TestSql()
    time_start = time.time()
    ts.test()
    time_end = time.time()
    # 运行时间
    print(time_end - time_start)
