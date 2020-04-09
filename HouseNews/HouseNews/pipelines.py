# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from twisted.enterprise import adbapi
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import datetime


class HousenewsPipeline(object):
    def __init__(self, dbpool):
        self._dbpool = dbpool
        self.pymssql_connect = pymssql.connect(host='10.10.202.12', database='TWSpider',
                                               user='bigdata', password='pUb6Qfv7BFxl', charset="utf8",
                                               )
        self.set_url_list = set()
        self.cursor = self.pymssql_connect.cursor()
        self.flag_match_rank = 0
        self.df_list_third = pd.DataFrame()
        self.sql_insert = '''
                Insert into ThirdHouseResource(LastTradingTime,TradingOwnerShip,PropertyYears,PropertyBelong,AreaName,PriceUnit,PropertyAddress,PropertyCity,PropertyWithinSquare,PropertyCommunity,BuildingType,BuildingStructure,BuildingSquare,TotalPrice,TotalFloor,HouseStructure,HouseYears,HouseType,HouseDirection,HouseUse,HouseCertificate,Floor,MortgageInfo,UpShelfDate,Resource,PlateName,LadderProtition,RentalStatus,FixTypeName,TimeToLive,TimeToRelease,HasGas,WaterType,ElectriciType,HasElevator,WatchHouse,LeaseTime,LeaseType,HasParkingPlace,HasHot,HouseStatus,HouseUrl,HouseDesc,BuildedTime,InsertTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''
        self.sql_update = "UPDATE ThirdHouseResource SET HouseStatus=%s, UpdateTime=%s where HouseUrl = %s"

    @classmethod
    def from_settings(cls, settings):
        dbpool = adbapi.ConnectionPool("pymssql", host='10.10.202.12', database='TWSpider',
                                       user='bigdata', password='pUb6Qfv7BFxl', charset="utf8",
                                       )
        return cls(dbpool)

    def process_item(self, item, spider):
        self._dbpool.runInteraction(self.do_select, item, spider)
        return item

    def do_insert(self, cursor, item):
        '''
        :param cursor:
        :param item:
        :return:
        '''
        input_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            self.sql_insert,
            (item.get("上次交易"), item.get("交易权属"), item.get("产权年限"),
             item.get("产权所属"), item.get("区域"), item.get("单价"),
             item.get("地址"), item.get("城市"), item.get("套内面积"),
             item.get("小区"), item.get("建筑类型"), item.get("建筑结构"),
             item.get("建筑面积"), item.get("总价"), item.get("总楼层"), item.get("户型结构"),
             item.get("房屋年限"), item.get("房屋户型"),
             item.get("房屋朝向"), item.get("房屋用途"), item.get("房本备件"), item.get("所在楼层"),
             item.get("抵押信息"), item.get("挂牌时间"),
             item.get("来源"), item.get("板块"), item.get("梯户比例"), item.get("租售状态"),
             item.get("装修情况"), item.get("入住"),
             item.get("发布"), item.get("燃气"), item.get("用水"), item.get("用电"),
             item.get("电梯"), item.get("看房"),
             item.get("租期"), item.get("租赁方式"), item.get("车位"), item.get("采暖"),
             item.get("房源状态"), item.get("房源链接"),
             item.get("房源描述"), item.get("建成时间"),
             input_time)
        )

    def do_update(self, cursor, item):
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            self.sql_update,
            (item.get('HouseStatus'), update_time, item.get('HouseUrl')
             )
        )

    def do_select(self, cursor, item, spider):
        flag_item = item.get("flag_item")
        if flag_item:
            # 更新状态：已售到可售
            print("可售数据:{}".format(item.get("房源描述")))
            self.do_update(cursor, item)
        else:
            house_url = item.get("房源链接")
            # 插入新的可售
            if house_url not in self.set_url_list:
                print("新增数据:{}".format(item.get("房源描述")))
                self.do_insert(cursor, item)
                self.set_url_list.add(house_url)

    def close_spider(self, spider):
        # 更新状态：可售到已售
        housing_trade_list = [x for x in spider.url_status_list if x.tolist() not in spider.url_status_list_available]
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for housing_trade in housing_trade_list:
            print("已售-已租数据:{}".format(housing_trade))
            房源链接 = housing_trade[0]
            房源状态 = housing_trade[1]
            if 房源状态 == "可售":
                self.cursor.execute(
                    self.sql_update,
                    ("已售", update_time, 房源链接
                     )
                )
            elif 房源状态 == '可租':
                self.cursor.execute(
                    self.sql_update,
                    ("已租", update_time, 房源链接
                     )
                )
            self.pymssql_connect.commit()
        self.cursor.close()
        self.pymssql_connect.close()
        self._dbpool.close()
