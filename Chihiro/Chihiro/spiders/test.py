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
    name = 'test'
    custom_settings = {
        # 基本参数
        # "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        'DEFAULT_REQUEST_HEADERS': {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Content-Length": "435",
            "Content-Type": "application/json;charset=UTF-8",
            "Cookie": "Hm_lvt_bf7258513fd26ee6d3231554baf1a331=1590384930; Hm_lpvt_bf7258513fd26ee6d3231554baf1a331=1590384980",
            "Host": "www.taiwu.com",
            "istravel": '0',
            "Origin": "http://www.taiwu.com",
            "Proxy-Connection": "keep-alive",
            "ref": '0',
            "Referer": "http://www.taiwu.com/ershoufang/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
        }
    }

    # base_url = "http://www.sinyi.com.cn/handhouse/"

    def start_requests(self):
        start_urls = 'http://www.taiwu.com/siteapi/outnetfront/api/property/PropertyListController/getPropertySecondHandList'
        data = {
            "areaCode": "",
            "averagePrice": [],
            "buildingAge": [],
            "buildingDirectionCode": [],
            "elevatorExist": [],
            "fixTypeCode": [],
            "gongge": 1,
            "intentionalMetroLineCode": "",
            "layerHighLowTypeCode": [],
            "leaseMode": [],
            "leasePriceCode": [],
            "pageNum": 2,
            "pageSize": 20,
            "propertyLabelFlg": [],
            "propertySquareCode": [],
            "propertySquareSort": None,
            "releaseDateSort": None,
            "roomNumberType": [],
            "roomTypeCode": [],
            "sellPriceCode": [],
            "sellPriceSort": None,
            "unitPriceSort": None,
        }
        data = json.dumps(data)
        # headers = {
        #
        # }
        for i in range(1, 3):
            yield scrapy.Request(url=start_urls,
                                 callback=self.parse, method="POST", body=data)

    def parse(self, response):
        res = response.json()
        print(res)
