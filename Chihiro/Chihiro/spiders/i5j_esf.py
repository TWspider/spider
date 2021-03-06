# -*- coding: utf-8 -*-
import scrapy
import re
import os
import json
import logging
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field
from selenium import webdriver
import sys

import pandas as pd
from sqlalchemy import create_engine
import datetime


class Chihiro(scrapy.Spider):
    name = 'i5j_esf'
    base_url = "https://sh.5i5j.com"
    start_urls = ['https://sh.5i5j.com/ershoufang/']
    PropertyCity = '上海'
    Resource = '我爱我家'
    RentalStatus = 0
    HouseStatus = '可售'
    custom_settings = {
        # 基本参数
        "BOT_NAME": name,
        "PropertyCity": PropertyCity,
        "Resource": Resource,
        "RentalStatus": RentalStatus,
        "HouseStatus": HouseStatus,
        # 请求参数
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            # 'Cookie': cookie,
            'Host': 'sh.5i5j.com',
            # 'Referer': 'https://sh.5i5j.com/ershoufang/',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        },
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 5,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            # 'Chihiro.middleware_request.ChihiroDownloaderMiddleware': 543,
            'Chihiro.middleware_request.IpAgent_Middleware': 222,
            'Chihiro.middleware_request.CookiesClear': 333,
        },
        # 清洗参数
        "SPIDER_MIDDLEWARES": {
            # 'Chihiro.middleware_item.ChihiroSpiderMiddleware': 500,

        },
        # 业务参数
        # "ITEM_PIPELINES": {
        #     'Chihiro.middleware_sql.ChihiroPipeline': 300,
        # },
        # 错误记录
        # ERROR_RECORD = True
        # 日志
        # "LOG_LEVEL": 'INFO',
        # "LOG_FILE": "Chihiro.txt"
    }

    def __init__(self):
        host = '10.10.202.13'
        user = 'bigdata_user'
        password = 'ulyhx3rxqhtw'

        # host = '10.55.5.7'
        # user = 'tw_user'
        # password = '123456'

        database = 'TWSpider'
        self.scaned_url_list = []
        self.engine_third_house = create_engine(
            'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where Resource='%s' and RentalStatus = %s" % (
                self.Resource, self.RentalStatus),
            self.engine_third_house)
        self.url_list = self.sql_select["HouseUrl"].tolist()

    def parse(self, response):
        region_xpath_list = response.xpath(
            "//ul[contains(@class,'new_di_tab')]/a[position()>1]")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./li/text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle,
                                 meta={"region": region})

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//div[contains(@class,'block')]/dl[contains(@class,'quyuCon')]/dd[@class='block']/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            plate_url = self.base_url + plate_url_handle
            yield scrapy.Request(url=plate_url, callback=self.house_handle,
                                 meta={"region": region, "plate": plate})

    def house_handle(self, response):
        housing_list = response.xpath("//div[@class='list-con-box'][1]/ul[@class='pList']/li/div[@class='listCon']")
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        for housing in housing_list:
            item = Item()
            item.fields["AreaName"] = Field()
            item["AreaName"] = region
            item.fields["PlateName"] = Field()
            item["PlateName"] = plate
            housing_url = housing.xpath("./h3/a/@href").extract_first()
            housing_url = self.base_url + housing_url
            housing_description = housing.xpath("./h3/a/text()").extract_first()
            housing_info = housing.xpath("./div[@class='listX']/p[1]/text()").extract_first().replace(" ", '')
            housing_info = housing_info.split("·")
            # 二手房为总价；租房为单价
            info_red = housing.xpath(
                "./div[@class='listX']/div[@class='jia']/p[@class='redC']/strong/text()").extract_first()
            info_grey = housing.xpath("./div[@class='listX']/div[@class='jia']/p[2]/text()").extract_first()
            item.fields["TotalPrice"] = Field()
            item.fields["PriceUnit"] = Field()
            flag_exception = None
            try:
                room_type = housing_info[0]
            except Exception as e:
                room_type = flag_exception
            try:
                room_area = housing_info[1]
            except Exception as e:
                room_area = flag_exception
            try:
                room_orientation = housing_info[2]
            except Exception as e:
                room_orientation = flag_exception
            try:
                location_floor = re.search("(.*?)/", housing_info[3]).group(1)
            except Exception as e:
                location_floor = flag_exception
            try:
                total_floor = re.search(".*?/(.*)", housing_info[3]).group(1)
            except Exception as e:
                total_floor = flag_exception
            try:
                room_decorate = housing_info[4]
            except Exception as e:
                room_decorate = flag_exception
            try:
                build_year = housing_info[5]
            except Exception as e:
                build_year = flag_exception
            item.fields["HouseDesc"] = Field()
            item["HouseDesc"] = housing_description
            item.fields["HouseUrl"] = Field()
            item["HouseUrl"] = housing_url
            # 房屋户型
            item.fields["HouseType"] = Field()
            item["HouseType"] = room_type
            # 建筑面积
            item.fields["BuildingSquare"] = Field()
            item["BuildingSquare"] = room_area
            # 房屋朝向
            item.fields["HouseDirection"] = Field()
            item["HouseDirection"] = room_orientation
            # 总楼层
            item.fields["TotalFloor"] = Field()
            item["TotalFloor"] = total_floor
            # 所在楼层
            item.fields["Floor"] = Field()
            item["Floor"] = location_floor
            # 装修情况
            item.fields["FixTypeName"] = Field()
            item["FixTypeName"] = room_decorate
            # 建成年份
            item.fields["BuildedTime"] = Field()
            item["BuildedTime"] = build_year
            # 小区
            community = housing.xpath("./div[@class='listX']/p[2]/a[1]/text()").extract_first()
            # 地址
            address = housing.xpath("./div[@class='listX']/p[2]/a[2]/text()").extract_first()
            if address:
                address = address.replace(" ", '')
            item.fields["PropertyCommunity"] = Field()
            item["PropertyCommunity"] = community
            item.fields["PropertyAddress"] = Field()
            item["PropertyAddress"] = address
            item["TotalPrice"] = info_red
            item["PriceUnit"] = info_grey.replace("单价", "")
            if self.is_finished():
                pipeline = self.crawler.spider.pipeline
                scaned_url_list = pipeline.scaned_url_list
                url_list = pipeline.url_list
                housing_trade_list = [x for x in url_list if x not in scaned_url_list]
                logging.info(housing_trade_list)
                for housing_url in housing_trade_list:
                    yield scrapy.Request(url=housing_url, callback=self.house_status_handle)

            yield item
        next_page_handle = response.xpath("//a[@class='cPage'][1]/@href").extract_first()
        if next_page_handle:
            next_page = self.base_url + next_page_handle
            yield scrapy.Request(url=next_page, callback=self.house_handle,
                                 meta={"region": region, "plate": plate})

    def is_finished(self):
        flag_queue = len(self.crawler.engine.slot.scheduler)
        if flag_queue:
            return False
        return True

    def house_status_handle(self, response):
        # 验证码
        url = response.url
        item = {}
        flag = response.xpath("//h1[@class='house-tit']/text()").extract_first()
        if flag:
            pass
        else:
            item["flag_remaining"] = True
            item["HouseUrl"] = url
            yield item
