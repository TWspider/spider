# -*- coding: utf-8 -*-
import scrapy
import re
import json
import pandas as pd
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
from sqlalchemy import create_engine
import datetime
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field


class Chihiro(scrapy.Spider):
    name = 'anjuke_zf'
    base_url = "http://www.sinyi.com.cn/handhouse/"
    start_urls = ['https://m.anjuke.com/sh/rent/all/a0_0-b0-0-0-f0/']
    PropertyCity = '上海'
    Resource = '安居客'
    RentalStatus = 1
    HouseStatus = '可租'
    custom_settings = {
        # 基本参数
        "BOT_NAME": name,
        "PropertyCity": PropertyCity,
        "Resource": Resource,
        "RentalStatus": RentalStatus,
        "HouseStatus": HouseStatus,
        # 请求参数
        "DEFAULT_REQUEST_HEADERS": {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': '*/*',
            'accept-language': 'gzip',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        },
        # "DOWNLOAD_DELAY": 0.3,
        # "CONCURRENT_REQUESTS": 1,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            # 'Chihiro.middleware_request.ChihiroDownloaderMiddleware': 543,
            'Chihiro.middleware_request.UserAgent_Middleware': 543,
        },
        # 清洗参数
        "SPIDER_MIDDLEWARES": {
            # 'Chihiro.middleware_item.ChihiroSpiderMiddleware': 500,
        },
        # 业务参数
        # "ITEM_PIPELINES": {
        # 'Chihiro.middleware_sql.ChihiroPipeline': 300,
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
        pass
        # AreaName          区域    pass
        # PlateName         板块    pass

        # PropertyCommunity 小区    pass
        # PropertyAddress   地址    pass

        # TotalFloor        总楼层    pass
        # Floor             所在楼层    pass

        # HouseUrl          链接    pass
        # HouseDesc         描述    pass

        # HouseType         户型    pass
        # BuildingSquare    面积    pass
        # TotalPrice        总价    pass
        # PriceUnit         单价    pass

        # HouseDirection    朝向    pass
        # FixTypeName       装修    pass
        # PubCompany        发布公司    pass
        # Agent             经纪人    pass
  