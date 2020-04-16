# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

import pymssql
import pandas as pd
from sqlalchemy import create_engine


class DafangyaPipeline(object):
    def open_spider(self, spider):
        self.conn = pymssql.connect("pymssql", host='10.10.202.12', database='TWSpider',
                                    user='bigdata', password='pUb6Qfv7BFxl', charset="utf8",
                                    )
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        print(item)
        insert_sql = "insert into ThirdHouseResource(PropertyAddress,AreaName,BuildingSquare,BuildedTime,PropertyCity,PropertyCommunity,FixTypeName,HouseDirection,HouseType,HasElevator,Floor,HouseUrl,HouseUse,InsertTime,PriceUnit,TimeToRelease,RentalStatus,Resource,HouseStatus,PlateName,TotalFloor,TotalPrice)" \
                     " values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
            item['addr'], item['area'], item['build_size'], item['build_time'], item['city']
            , item['community'], item['decoration'], item['direction'], item['door_model'],
            item['elevator']
            , item['floor'], item['house_url'], item['house_use'], item['insert_time'],
            item['per_price'],
            item['release_time']
            , item['rental_status'], item['resource'], item['resource_status'], item['road'],
            item['total_floor'], item['total_price']
        )
        self.cur.execute(insert_sql)
        self.conn.commit()
        return item

    def close_spider(self, spider):
        print('close_spider', datetime.datetime.now())
        self.conn.close()
        self.cur.close()
