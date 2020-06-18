# -*- coding: utf-8 -*-
import scrapy
import re
import json
import logging
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import pandas as pd
from sqlalchemy import create_engine
import datetime


class Chihiro(scrapy.Spider):
    name = 'xinyi_zf'
    base_url = "http://www.sinyi.com.cn/rent/"
    start_urls = ['http://www.sinyi.com.cn/rent/list']
    PropertyCity = '上海'
    Resource = '信义房屋'
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
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'gzip',
            'Upgrade-Insecure-Requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        },
        # "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 10,
        "RETRY_HTTP_CODES": [403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            # 'Chihiro.middleware_request.ChihiroDownloaderMiddleware': 543,
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
        pagesize = response.xpath("//div[contains(@class,'h40')]/div/span/text()").extract_first()
        url = "http://www.sinyi.com.cn/rent/list?page=1&pagesize=" + pagesize
        yield scrapy.Request(url=url, callback=self.handle_1)

    def handle_1(self, response):
        housing_list = response.xpath("//ul[@class='wp100']/li[contains(@class,'wp100')]/div")
        for housing in housing_list:
            i = ItemLoader(item=ChihiroItem(), selector=housing)
            # HouseUrl          链接    pass
            HouseUrl = housing.xpath("./div/a/@href").extract_first()
            HouseUrl = self.base_url + HouseUrl.replace("&cookieuid={{cookieuid}}", '')
            i.add_value("HouseUrl", HouseUrl)
            # HouseType         户型    pass
            i.add_xpath("HouseType", "./div[contains(@class,'wp100')]/div/p[@class='h30'][1]/span[1]/text()")
            # BuildingSquare    面积    pass
            i.add_xpath("BuildingSquare", "./div[contains(@class,'wp100')]/div/p[@class='h30'][1]/span[2]/text()")

            # HouseDirection    朝向    pass
            i.add_xpath("HouseDirection", "./div[contains(@class,'wp100')]/div/p[@class='h30'][1]/span[3]/text()")

            # BuildedTime       建成时间    pass
            i.add_xpath("BuildedTime", "./div[contains(@class,'wp100')]/div/p[@class='h30'][1]/span[4]/text()")

            # PriceUnit         单价    pass
            i.add_xpath("PriceUnit", "./div[contains(@class,'wp100')]/div[2]/span/text()")

            item = i.load_item()
            yield scrapy.Request(url=HouseUrl, callback=self.handle_2, meta={"item": deepcopy(item)})

    def handle_2(self, response):
        # AreaName          区域    pass
        item = response.meta.get("item")
        i = ItemLoader(item=ChihiroItem(), response=response)
        res = response.xpath("//ul[contains(@class,'wp100')]/li[contains(@class,'wp100')][2]/a/text()").extract_first()
        AreaName = re.search("\[(.*?) ", res).group(1)
        i.add_value("AreaName", AreaName)
        # PlateName         板块    pass
        PlateName = re.search(" (.*)\]", res).group(1)
        i.add_value("PlateName", PlateName)
        # PropertyCommunity 小区    pass
        i.add_xpath("PropertyCommunity", "//span[@class='col-28AA35']/text()")
        # PropertyAddress   地址    pass
        PropertyAddress = re.search("\](.*)", res).group(1)
        i.add_value("PropertyAddress", PropertyAddress)
        # TotalFloor        总楼层    pass
        res1 = response.xpath(
            "//ul[contains(@class,'wp100')]/li[contains(@class,'wp52')][2]/span[contains(@class,'col-666333')]/text()").extract_first()
        TotalFloor = re.search("/(.*)层", res1).group(1)
        i.add_value("TotalFloor", TotalFloor)
        # Floor             所在楼层    pass
        Floor = re.search("(.*)/", res1).group(1)
        i.add_value("Floor", Floor)
        # HouseDesc         描述    pass
        i.add_xpath("HouseDesc", "//p[@class='h40']/span/text()")
        # FixTypeName       装修    pass
        i.add_xpath("FixTypeName",
                    "//ul[contains(@class,'wp100')]/li[contains(@class,'wp100')][1]/span[contains(@class,'pl3')]/text()")
        # Agent             经纪人    pass
        i.add_xpath("Agent", "//p[contains(@class,'col-333')]/a/span/text()")

        item1 = i.load_item()
        item1.update(item)
        if self.is_finished():
            pipeline = self.crawler.spider.pipeline
            scaned_url_list = pipeline.scaned_url_list
            url_list = pipeline.url_list
            housing_trade_list = [x for x in url_list if x not in scaned_url_list]
            logging.info(housing_trade_list)
            for housing_url in housing_trade_list:
                yield scrapy.Request(url=housing_url, callback=self.house_status_handle)
        yield item

    def is_finished(self):
        flag_queue = len(self.crawler.engine.slot.scheduler)
        if flag_queue:
            return False
        return True

    def house_status_handle(self, response):
        # 验证码
        url = response.url
        item = {}
        flag = response.xpath("//p[@class='h40']/span/text()").extract_first()
        if flag:
            pass
        else:
            item["flag_remaining"] = True
            item["HouseUrl"] = url
            yield item
