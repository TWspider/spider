import pymssql
import datetime
from DBUtils.PooledDB import PooledDB
import pandas as pd
from sqlalchemy import create_engine
from TW.spider.Test_spider.apps.house.config import *


class SqlHandle(object):
    __pool = None

    def __init__(self, table_name, flag_match_rank):
        self.conn = SqlHandle.getsqlconn()
        self.cursor = self.conn.cursor()
        self.table = table_name
        # 状态变更
        self.engine_third_house = create_engine('mssql+pymssql://tw_user:123456@10.55.5.7/TWSpider')
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where RESOURCE=%s" % RESOURCE,
            self.engine_third_house)
        self.url_status_list = self.sql_select.values
        self.url_list = self.sql_select["HouseUrl"].to_list()
        self.sql_insert = '''
        Insert into ThirdHouseResource(LastTradingTime,TradingOwnerShip,PropertyYears,PropertyBelong,AreaName,PriceUnit,PropertyAddress,PropertyCity,PropertyWithinSquare,PropertyCommunity,BuildingType,BuildingStructure,BuildingSquare,TotalPrice,TotalFloor,HouseStructure,HouseYears,HouseType,HouseDirection,HouseUse,HouseCertificate,Floor,MortgageInfo,UpShelfDate,Resource,PlateName,LadderProtition,RentalStatus,FixTypeName,TimeToLive,TimeToRelease,HasGas,WaterType,ElectriciType,HasElevator,WatchHouse,LeaseTime,LeaseType,HasParkingPlace,HasHot,HouseStatus,HouseUrl,HouseDesc,BuildedTime,InsertTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.sql_update = "UPDATE {} SET HouseStatus=%s, UpdateTime=%s where HouseUrl = %s".format(self.table)

        # 星级匹配
        self.flag_match_rank = flag_match_rank
        if flag_match_rank:
            pass

    # 数据库连接池连接
    @staticmethod
    def getsqlconn():
        '''
        建立连接池
        :return:
        '''
        if SqlHandle.__pool is None:
            __pool = PooledDB(pymssql, 20, host='10.55.5.215', user='tw_user', password='123456', database='TWSpider')
        return __pool.connection()

    def do_insert(self, item):
        '''
        插入数据
        :param item:
        :return:
        '''
        sql_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cursor.execute(self.sql_insert, (
            item.get("LastTradingTime"), item.get("TradingOwnerShip"), item.get("PropertyYears"),
            item.get("PropertyBelong"), item.get("AreaName"), item.get("PriceUnit"),
            item.get("PropertyAddress"), item.get("PropertyCity"), item.get("PropertyWithinSquare"),
            item.get("PropertyCommunity"), item.get("BuildingType"), item.get("BuildingStructure"),
            item.get("BuildingSquare"), item.get("TotalPrice"), item.get("TotalFloor"), item.get("HouseStructure"),
            item.get("HouseYears"), item.get("HouseType"),
            item.get("HouseDirection"), item.get("HouseUse"), item.get("HouseCertificate"), item.get("Floor"),
            item.get("MortgageInfo"), item.get("UpShelfDate"),
            item.get("Resource"), item.get("PlateName"), item.get("LadderProtition"), item.get("RentalStatus"),
            item.get("FixTypeName"), item.get("TimeToLive"),
            item.get("TimeToRelease"), item.get("HasGas"), item.get("WaterType"), item.get("ElectriciType"),
            item.get("HasElevator"), item.get("WatchHouse"),
            item.get("LeaseTime"), item.get("LeaseType"), item.get("HasParkingPlace"), item.get("HasHot"),
            item.get("HouseStatus"), item.get("HouseUrl"),
            item.get("HouseDesc"), item.get("BuildedTime"), sql_time,
        ))
        self.conn.commit()

    def do_update(self, item):
        '''
        更新数据
        :param item:
        :return:
        '''
        sql_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cursor.execute(self.sql_update, (item.get("HouseStatus"), sql_time, item.get("HouseUrl")))
        self.conn.commit()

    def match_rank(self, item):
        '''
        业务：星级匹配
        :param item:
        :return:
        '''
        pass

    def do_sql(self, item):
        '''
        业务主线：状态变更、星级匹配
        :param item:
        :return:
        '''
        HouseUrl = item.get("HouseUrl")
        HouseStatus = item.get("HouseStatus")
        url_status = [HouseUrl, HouseStatus]
        # 判断库内是否存在
        if url_status not in self.url_status_list:
            if HouseUrl in self.url_status_list:
                # 更新已售为可售
                print("成交数据:{}".format(item.get("房源描述")))
                self.do_update(item)
            else:
                # 新增数据
                print("插入数据:{}".format(item.get("房源描述")))
                self.do_insert(item)
                # 星级匹配
                if self.flag_match_rank:
                    pass

    def close(self):
        self.cursor.close()
        self.conn.close()


sh = SqlHandle(table_name="temp", flag_match_rank=0)

if __name__ == "__main__":
    pass
else:
    pass
