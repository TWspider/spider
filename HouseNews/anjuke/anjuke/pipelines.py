# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

import pymssql


class AnjukePipeline(object):

    def open_spider(self, spider):
        self.count = 0
        self.conn = pymssql.connect("pymssql", host='10.10.202.12', database='TWSpider',
                                    user='bigdata', password='pUb6Qfv7BFxl', charset="utf8",
                                    )
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        print(item['Pub_company'], 'pipline====================')
        self.count += 1
        print(self.count)
        insert_sql = "insert into ThirdHouseResource(PropertyAddress,AreaName,BuildingSquare,BuildedTime,PropertyCity,PropertyCommunity,FixTypeName,HouseDirection,HouseType,HasElevator,Floor,HouseUrl,InsertTime,PriceUnit,RentalStatus,Resource,HouseStatus,TotalFloor,TotalPrice,HouseDesc,PubCompany,Agent,PropertyYears,HouseYears)" \
                     " values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
            item['addr'], item['area'], item['build_size'], item['build_time'], item['city']
            , item['community'], item['decoration'], item['direction'], item['door_model'],
            item['elevator']
            , item['floor'], item['house_url'], item['insert_time'],
            item['per_price']
            , item['rental_status'], item['resource'], item['resource_status'],
            item['total_floor'], item['total_price'], item['house_desc'], item['Pub_company'],
            item['agent'], item['property_year'], item['house_years']
        )
        self.cur.execute(insert_sql)
        # self.conn.commit()
        return item

    def close_spider(self, spider):
        print(self.count, '-------------------------------')
        print('close_spider', datetime.datetime.now())
        self.conn.close()
        self.cur.close()




from twisted.enterprise import adbapi
import pymssql
import datetime
import pandas as pd
from sqlalchemy.engine import create_engine


class AnjukeZufangPipeline(object):
    def __init__(self, dbpool):
        self._dbpool = dbpool
        # 执行最终变更状态
        self.pymssql_connect = pymssql.connect(host='10.10.202.12', database='TWSpider',
                                               user='bigdata', password='pUb6Qfv7BFxl', charset="utf8",
                                               )
        self.cursor = self.pymssql_connect.cursor()
        self.url_status_list_available = []
        self.source = "安居客"

        # 执行状态变更及去重
        self.engine_third_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWSpider')
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where Resource='%s' and RentalStatus = 1" % self.source,
            self.engine_third_house)
        self.url_list = self.sql_select["HouseUrl"].to_list()
        self.url_status_list = self.sql_select.values
        # 防止页面重复导致的数据重复
        self.set_url_list = set()
        # 插入及更新函数
        self.sql_insert = '''
                  Insert into ThirdHouseResource(AreaName,PropertyCity,Resource,PlateName,HouseUrl,HouseDesc,HouseType,BuildingSquare,PubCompany,Agent,InsertTime,PriceUnit,PropertyCommunity,HouseDirection,TotalFloor,Floor,HouseDirection,RentalStatus,HouseStatus) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
        cursor.execute(
            self.sql_insert,
            (item.get("area"), item.get("city"), item.get("resource"),
             item.get("plate"), item.get("house_url"), item.get("house_desc"),
             item.get("door_model"), item.get("build_size"), item.get("Pub_company"),
             item.get("agent"), item.get("insert_time"), item.get("per_price"),
             item.get("community"), item.get("direction"), item.get("total_floor"), item.get("floor"),
             item.get("decoration"), item.get("rental_status"),
             item.get("resource_status"))
        )

    def do_update(self, cursor, item):
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            self.sql_update,
            (item.get('resource_status'), update_time, item.get('house_url')
             )
        )

    def do_select(self, cursor, item, spider):
        housing_url = item.get('house_url')
        house_status = item.get("resource_status")
        flag = [housing_url, house_status]
        if housing_url in self.url_list:
            if flag in self.url_status_list:
                self.url_status_list_available.append(flag)
            else:
                # 更新已售为可售
                print("历史房源:{}".format(item.get("house_desc")))
                self.do_update(cursor, item)
                self.url_status_list_available.append([housing_url, '已售'])
        else:
            # 插入新的可售
            if housing_url not in self.set_url_list:
                print("新增房源:{}".format(item.get("house_desc")))
                self.do_insert(cursor, item)
                self.set_url_list.add(housing_url)

    def close_spider(self, spider):
        # 更新状态：可售到已售
        self._dbpool.close()
        housing_trade_list = [x for x in spider.url_status_list if x.tolist() not in spider.url_status_list_available]
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for housing_trade in housing_trade_list:
            print("下架房源:{}".format(housing_trade))
            房源链接 = housing_trade[0]
            self.cursor.execute(
                self.sql_update,
                ("已租", update_time, 房源链接
                 )
            )
            self.pymssql_connect.commit()
        self.cursor.close()
        self.pymssql_connect.close()
