# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from twisted.enterprise import adbapi
import pandas as pd
from sqlalchemy import create_engine
import datetime
from scrapy import Field


# 开发5.7
# 10.55.5.7
# tw_user
# 123456
# 状态变更
host = '10.10.202.13'
user = 'bigdata_user'
password = 'ulyhx3rxqhtw'

# host = '10.55.5.7'
# user = 'tw_user'
# password = '123456'

database = 'TWSpider'

class ChihiroPipeline(object):
    def __init__(self, dbpool, settings):
        '''
LastTradingTime,TradingOwnerShip,PropertyYears,PropertyBelong,AreaName,PriceUnit,PropertyAddress,PropertyCity,
PropertyWithinSquare,PropertyCommunity,BuildingType,BuildingStructure,BuildingSquare,TotalPrice,TotalFloor,HouseStructure,
HouseYears,HouseType,HouseDirection,HouseUse,HouseCertificate,Floor,MortgageInfo,UpShelfDate,Resource,PlateName,LadderProtition,
RentalStatus,FixTypeName,TimeToLive,TimeToRelease,HasGas,WaterType,ElectriciType,HasElevator,
WatchHouse,LeaseTime,LeaseType,HasParkingPlace,HasHot,InsertTime,UpdateTime,
HouseStatus,HouseUrl,HouseDesc,BuildedTime,PubCompany,Agent
        :param dbpool:
        :param settings:
        '''
        self._dbpool = dbpool
        self.scaned_url_list = []
        self.PropertyCity = settings.get("PropertyCity")
        self.Resource = settings.get("Resource")
        self.RentalStatus = settings.get("RentalStatus")
        self.HouseStatus = settings.get("HouseStatus")
        # 执行页面去重
        self.set_url_list = set()

        self.engine_third_house = create_engine(
            'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where Resource='%s' and RentalStatus = %s" % (
                self.Resource, self.RentalStatus),
            self.engine_third_house)
        self.url_list = self.sql_select["HouseUrl"].tolist()
        self.url_status_list = self.sql_select.values.tolist()
        self.sql_insert = '''
                  Insert into ThirdHouseResource(LastTradingTime,TradingOwnerShip,PropertyYears,PropertyBelong,AreaName,PriceUnit,PropertyAddress,PropertyCity,
PropertyWithinSquare,PropertyCommunity,BuildingType,BuildingStructure,BuildingSquare,TotalPrice,TotalFloor,HouseStructure,
HouseYears,HouseType,HouseDirection,HouseUse,HouseCertificate,Floor,MortgageInfo,UpShelfDate,Resource,PlateName,LadderProtition,
RentalStatus,FixTypeName,TimeToLive,TimeToRelease,HasGas,WaterType,ElectriciType,HasElevator,
WatchHouse,LeaseTime,LeaseType,HasParkingPlace,HasHot,InsertTime,UpdateTime,
HouseStatus,HouseUrl,HouseDesc,BuildedTime,PubCompany,Agent) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                  '''
        self.sql_update = "UPDATE ThirdHouseResource SET HouseStatus=%s, UpdateTime=%s where HouseUrl = %s"

    @classmethod
    def from_settings(cls, settings):
        '''
        # 开发5.7
        # 10.55.5.7
        # tw_user
        # 123456
        # 爬虫13
        # 10.10.202.13
        # bigdata_user
        # ulyhx3rxqhtw
        :param settings:
        :return:
        '''
        dbpool = adbapi.ConnectionPool("pymssql", host=host, database=database,
                                       user=user, password=password, charset="utf8",
                                       )
        return cls(dbpool, settings)

    def process_item(self, item, spider):
        self._dbpool.runInteraction(self.do_select, item, spider)
        return item

    def do_insert(self, cursor, item):
        '''
        :param cursor:
        :param item:
        :return:
        '''
        cursor.execute(
            self.sql_insert,
            (
                item.get("LastTradingTime"), item.get("TradingOwnerShip"), item.get("PropertyYears"),
                item.get("PropertyBelong"), item.get("AreaName"), item.get("PriceUnit"),
                item.get("PropertyAddress"), item.get("PropertyCity"), item.get("PropertyWithinSquare"),
                item.get("PropertyCommunity"), item.get("BuildingType"), item.get("BuildingStructure"),
                item.get("BuildingSquare"), item.get("TotalPrice"), item.get("TotalFloor"), item.get("HouseStructure"),
                item.get("HouseYears"), item.get("HouseType"), item.get("HouseDirection"), item.get("HouseUse"),
                item.get("HouseCertificate"), item.get("Floor"), item.get("MortgageInfo"), item.get("UpShelfDate"),
                item.get("Resource"), item.get("PlateName"), item.get("LadderProtition"), item.get("RentalStatus"),
                item.get("FixTypeName"), item.get("TimeToLive"), item.get("TimeToRelease"), item.get("HasGas"),
                item.get("WaterType"), item.get("ElectriciType"), item.get("HasElevator"), item.get("WatchHouse"),
                item.get("LeaseTime"), item.get("LeaseType"), item.get("HasParkingPlace"), item.get("HasHot"),
                item.get("InsertTime"), item.get("UpdateTime"), item.get("HouseStatus"), item.get("HouseUrl"),
                item.get("HouseDesc"), item.get("BuildedTime"), item.get("PubCompany"), item.get("Agent"),
            )
        )

    def do_update(self, cursor, item):
        cursor.execute(
            self.sql_update,
            (
                item.get('HouseStatus'), item.get("UpdateTime"), item.get('HouseUrl')
            )
        )

    def do_select(self, cursor, item, spider):
        housing_url = item.get('HouseUrl')
        flag = [housing_url, self.HouseStatus]
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 判断是否新增
        if housing_url in self.url_list:
            # 判断是否状态变更
            if flag in self.url_status_list:
                # 状态不变
                self.scaned_url_list.append(housing_url)
            else:
                # 更新已售为可售、已租为可租
                item.fields["UpdateTime"] = Field()
                item["UpdateTime"] = t
                item.fields["HouseStatus"] = Field()
                item["HouseStatus"] = self.HouseStatus
                print("历史房源:{}".format(item.get("HouseDesc")))
                self.do_update(cursor, item)
                self.scaned_url_list.append(housing_url)
        else:
            # 插入新的可售
            item.fields["InsertTime"] = Field()
            item.fields["PropertyCity"] = Field()
            item.fields["Resource"] = Field()
            item.fields["RentalStatus"] = Field()
            item.fields["HouseStatus"] = Field()
            item["InsertTime"] = t
            item["PropertyCity"] = self.PropertyCity
            item["Resource"] = self.Resource
            item["RentalStatus"] = self.RentalStatus
            item["HouseStatus"] = self.HouseStatus
            if housing_url not in self.set_url_list:
                print("新增房源:{}".format(item.get("HouseDesc")))
                self.do_insert(cursor, item)
                self.set_url_list.add(housing_url)

    def do_final(self, cursor, spider):
        housing_trade_list = [x for x in self.url_list if x not in self.scaned_url_list]
        for housing_url in housing_trade_list:
            update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if self.RentalStatus:
                house_status = "已租"
            else:
                house_status = "已售"
            print("已下架的:{}{}".format(housing_url, house_status))
            cursor.execute(
                self.sql_update,
                (
                    house_status, update_time, housing_url
                )
            )

    def close_spider(self, cursor):
        self._dbpool.runInteraction(self.do_final, cursor)
        self._dbpool.close()
