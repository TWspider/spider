# -*- coding: utf-8 -*-
import scrapy
import re
import json
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
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
        "ITEM_PIPELINES": {
            # 'Chihiro.middleware_sql.ChihiroPipeline': 300,
        },
        # 错误记录
        # ERROR_RECORD = True
        # 日志
        # "LOG_LEVEL": 'INFO',
        # "LOG_FILE": "Chihiro.txt"
    }

    def __init__(self):
        self.headers = [
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
            'Opera/9.25 (Windows NT 5.1; U; en)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
            'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
        ]

    def get_headers(self):
        settings = get_project_settings()
        user_agent = {"User-Agent": random.choice(self.headers)}
        headers = user_agent.update(settings.get("DEFAULT_REQUEST_HEADERS"))
        return headers

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
  