# -*- coding: utf-8 -*-
import scrapy
import re
import logging
import json
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
import pandas as pd
from sqlalchemy import create_engine
import datetime
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field

from ..middleware_sql import ChihiroPipeline
class Chihiro(scrapy.Spider):
    name = 'lianjia_zf'
    base_url = "https://sh.lianjia.com"
    start_urls = ['https://sh.lianjia.com/zufang/']
    PropertyCity = '上海'
    Resource = '链家'
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
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        },
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 5,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            'Chihiro.middleware_request.IpAgent_Middleware': 543,
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
        region_xpath_list = response.xpath(
            "//ul[2]/li[@class='filter__item--level2  ']/a")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle, meta={"region": region},
                                 headers=self.get_headers())

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//ul[4]/li[@class='filter__item--level3  ']/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            plate_url = self.base_url + plate_url_handle
            yield scrapy.Request(url=plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate, "plate_url": plate_url},
                                 headers=self.get_headers(), dont_filter=True)

    def page_handle(self, response):
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        plate_url = response.meta.get("plate_url")
        housing_num_flag = response.xpath("//span[@class='content__title--hl']/text()").extract_first()
        if housing_num_flag != None:
            if housing_num_flag != '0':
                housing_list = response.xpath(
                    "//div[@class='content__list--item--main']/p[contains(@class,'content__list--item--title')]")
                for housing in housing_list:
                    # 列表页面字段获取
                    item = Item()
                    item.fields["AreaName"] = Field()
                    item["AreaName"] = region
                    item.fields["PlateName"] = Field()
                    item["PlateName"] = plate
                    housing_url = housing.xpath("./a/@href").extract_first()
                    housing_url = self.base_url + housing_url
                    HouseDesc = housing.xpath("./a/text()").extract_first()
                    if HouseDesc:
                        HouseDesc = HouseDesc.replace(" ", "").replace("\n", "")
                    item.fields["HouseDesc"] = Field()
                    item["HouseDesc"] = HouseDesc
                    item.fields["HouseUrl"] = Field()
                    item["HouseUrl"] = housing_url
                    yield scrapy.Request(url=housing_url, callback=self.housing_handle,
                                         meta={"item": deepcopy(item)},
                                         headers=self.get_headers())
                try:
                    total_page = int(response.xpath(
                        "//div[contains(@class, 'content__pg')]/@data-totalpage").extract_first())
                    current_page = int(response.xpath(
                        "//div[contains(@class, 'content__pg')]/@data-curpage").extract_first())
                    if current_page < total_page:
                        next_page = plate_url + "pg" + str(current_page + 1) + "/"
                        yield scrapy.Request(url=next_page, callback=self.page_handle,
                                             meta={"plate_url": plate_url, 'region': region,
                                                   "plate": plate}, headers=self.get_headers(),
                                             dont_filter=True)
                except:
                    pass

    def housing_handle(self, response):
        item = response.meta.get("item")
        item1 = Item()
        # 单价
        unit_price = response.xpath("//div[@class='content__aside--title']/span/text()").extract_first()
        # 租赁方式
        lease_way = response.xpath("//ul[@class='content__aside__list']/li[1]/text()").extract_first()
        # 房屋户型
        room_type = response.xpath("//ul[@class='content__aside__list']/li[2]/text()").extract_first()
        if room_type:
            room_type = re.search("(.*?) (.*)", room_type).group(1)
        item1.fields["PriceUnit"] = Field()
        item1["PriceUnit"] = unit_price
        item1.fields["LeaseType"] = Field()
        item1["LeaseType"] = lease_way
        item1.fields["HouseType"] = Field()
        item1["HouseType"] = room_type

        # 详细信息
        base_detail_info_list = response.xpath(
            "//div[@class='content__article__info']/ul/li[contains(text(),'：')]/text()")
        for base_detail_info in base_detail_info_list:
            base_detail_info_str = base_detail_info.extract()
            base_key_info = re.search("(.*?)：", base_detail_info_str).group(1)
            base_value_info = re.search(".*?：(.*)", base_detail_info_str).group(1)
            if base_key_info != "楼层":
                if base_key_info == "车位":
                    base_key_info = "HasParkingPlace"
                elif base_key_info == "用电":
                    base_key_info = "ElectriciType"
                elif base_key_info == "采暖":
                    base_key_info = "HasHot"
                elif base_key_info == "租期":
                    base_key_info = "LeaseTime"
                elif base_key_info == "看房":
                    base_key_info = "WatchHouse"
                elif base_key_info == "入住":
                    base_key_info = "TimeToLive"
                elif base_key_info == "电梯":
                    base_key_info = "HasElevator"
                elif base_key_info == "用水":
                    base_key_info = "WaterType"
                elif base_key_info == "燃气":
                    base_key_info = "HasGas"
                elif base_key_info == "面积":
                    base_key_info = "BuildingSquare"
                elif base_key_info == "朝向":
                    base_key_info = "HouseDirection"
                item1.fields[base_key_info] = Field()
                item1[base_key_info] = base_value_info
            else:
                # 总楼层
                location_floor = re.search("(.*?)/", base_value_info).group(1)
                # 所在楼层
                total_floor = re.search(".*?/(.*)", base_value_info).group(1)
                item1.fields["TotalFloor"] = Field()
                item1["TotalFloor"] = total_floor
                item1.fields["Floor"] = Field()
                item1["Floor"] = location_floor
        item1.update(item)
        if self.is_finished():
            # crawler
            pipeline = self.crawler.spider.pipeline
            scaned_url_list = pipeline.scaned_url_list
            url_list = pipeline.url_list
            housing_trade_list = [x for x in url_list if x not in scaned_url_list]
            logging.info(housing_trade_list)
            for housing_url in housing_trade_list:
                yield scrapy.Request(url=housing_url, callback=self.house_status_handle, headers=self.get_headers())
        yield item1

    def is_finished(self):
        flag_queue = len(self.crawler.engine.slot.scheduler)
        if flag_queue:
            return False
        return True

    def house_status_handle(self, response):
        # 验证码
        url = response.url
        item = {}
        flag = response.xpath("//p[@class='content__title']/text()").extract_first()
        if flag:
            pass
        else:
            item["flag_remaining"] = True
            item["HouseUrl"] = url
            yield item