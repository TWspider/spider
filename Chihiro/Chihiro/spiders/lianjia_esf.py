# -*- coding: utf-8 -*-
import scrapy
import re
import pandas as pd
from sqlalchemy import create_engine
import datetime
import logging

import json
from copy import deepcopy

from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field


class Chihiro(scrapy.Spider):
    name = 'lianjia_esf'
    base_url = "https://sh.lianjia.com"
    start_urls = ['https://sh.lianjia.com/ershoufang/']
    PropertyCity = '上海'
    Resource = '链家'
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

    def is_finished(self):
        if self.crawler.engine.downloader.active:
            return False
        if self.crawler.engine.slot.start_requests is not None:
            return False
        if self.crawler.engine.slot.scheduler.has_pending_requests():
            return False
        return True

    def get_headers(self):
        settings = get_project_settings()
        user_agent = {"User-Agent": random.choice(self.headers)}
        headers = user_agent.update(settings.get("DEFAULT_REQUEST_HEADERS"))
        return headers

    def parse(self, response):
        region_xpath_list = response.xpath(
            "//div[3]/div[@class='m-filter']/div[@class='position']/dl[2]/dd/div[1]/div/a")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle, meta={"region": region},
                                 headers=self.get_headers())

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//dl[2]/dd/div[1]/div[2]/a")
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
        try:
            housing_num_flag = response.xpath("//h2[contains(@class,'total')]/span/text()").extract_first().strip()
        except:
            housing_num_flag = '0'
        if housing_num_flag != "0":
            housing_list = response.xpath(
                "//div[@class='leftContent']/ul[@class='sellListContent']/li[contains(@class, 'clear')]/div[@class='info clear']")
            for housing in housing_list:
                # 列表页面字段获取
                item = Item()
                item.fields["AreaName"] = Field()
                item["AreaName"] = region
                item.fields["PlateName"] = Field()
                item["PlateName"] = plate
                housing_url = housing.xpath("./div[@class='title']/a/@href").extract_first()
                HouseDesc = housing.xpath("./div[@class='title']/a/text()").extract_first()
                item.fields["HouseDesc"] = Field()
                item["HouseDesc"] = HouseDesc
                item.fields["HouseUrl"] = Field()
                item["HouseUrl"] = housing_url
                yield scrapy.Request(url=housing_url, callback=self.housing_handle,
                                     meta={"item": deepcopy(item)},
                                     headers=self.get_headers())
            page_dict_handle = response.xpath(
                "//div[contains(@class, 'page-box')]/@page-data").extract_first()
            page_dict = json.loads(page_dict_handle)
            total_page = page_dict.get("totalPage")
            current_page = page_dict.get("curPage")
            if current_page < total_page:
                next_page = plate_url + "pg" + str(current_page + 1) + "/"
                yield scrapy.Request(url=next_page, callback=self.page_handle,
                                     meta={"plate_url": plate_url, 'region': region,
                                           "plate": plate}, headers=self.get_headers(),
                                     )

    def housing_handle(self, response):
        item = response.meta.get("item")
        item1 = Item()
        # 小区
        community = response.xpath(
            "//div[@class='communityName']/a[contains(@class,'info')]/text()").extract_first()
        # 地址
        address = response.xpath("//div[@class='areaName']").xpath("string(.)").extract_first()
        try:
            address = address.replace("所在区域", "").replace("\xa0", '-')
        except:
            address = None
        # 总价
        total_price = response.xpath("//div[contains(@class,'price')]/span[@class='total']/text()").extract_first()
        # 单价
        unit_price = response.xpath("//span[@class='unitPriceValue']/text()").extract_first()

        floor_info = response.xpath("//div[@class='room']/div[@class='subInfo']/text()").extract_first()
        # 总楼层
        if floor_info:
            total_floor = re.search("(共(.*?)层)", floor_info)
            if total_floor:
                total_floor = total_floor.group(1)
            # 所在楼层
            location_floor = re.search("(.*?)/", floor_info)
            if location_floor:
                location_floor = location_floor.group(1)
        else:
            total_floor = None
            location_floor = None
        item1.fields["TotalFloor"] = Field()
        item1["TotalFloor"] = total_floor
        item1.fields["Floor"] = Field()
        item1["Floor"] = location_floor
        item1.fields["PropertyCommunity"] = Field()
        item1["PropertyCommunity"] = community
        item1.fields["PropertyAddress"] = Field()
        item1["PropertyAddress"] = address
        item1.fields["TotalPrice"] = Field()
        item1["TotalPrice"] = total_price
        item1.fields["PriceUnit"] = Field()
        item1["PriceUnit"] = unit_price
        # 基本信息
        base_detail_info_list = response.xpath("//div[@class='base']/div[@class='content']/ul/li")
        for base_detail_info in base_detail_info_list:
            base_key_info = base_detail_info.xpath("./span/text()").extract_first()
            base_value_info = base_detail_info.xpath("./text()").extract_first()
            if base_key_info != "所在楼层":
                if base_key_info == "房屋户型":
                    base_key_info = "HouseType"
                elif base_key_info == "建筑面积":
                    base_key_info = "BuildingSquare"
                elif base_key_info == "套内面积":
                    base_key_info = "PropertyWithinSquare"
                elif base_key_info == "房屋朝向":
                    base_key_info = "HouseDirection"
                elif base_key_info == "装修情况":
                    base_key_info = "FixTypeName"
                elif base_key_info == "配备电梯":
                    base_key_info = "HasElevator"
                elif base_key_info == "户型结构":
                    base_key_info = "HouseStructure"
                elif base_key_info == "建筑类型":
                    base_key_info = "BuildingType"
                elif base_key_info == "建筑结构":
                    base_key_info = "BuildingStructure"
                elif base_key_info == "梯户比例":
                    base_key_info = "LadderProtition"
                elif base_key_info == "产权年限":
                    base_key_info = "PropertyYears"

                item1.fields[base_key_info] = Field()
                item1[base_key_info] = base_value_info

        # 交易属性
        trade_detail_info_list = response.xpath("//div[@class='transaction']/div[@class='content']/ul/li")
        for trade_detail_info in trade_detail_info_list:
            trade_info = trade_detail_info.xpath("./span/text()").extract()
            trade_key_info = trade_info[0]
            trade_value_info = trade_info[1]
            if trade_key_info == "抵押信息":
                trade_value_info = trade_value_info.strip()

            if trade_key_info == "挂牌时间":
                trade_key_info = "UpShelfDate"
            elif trade_key_info == "交易权属":
                trade_key_info = "TradingOwnerShip"
            elif trade_key_info == "上次交易":
                trade_key_info = "LastTradingTime"
            elif trade_key_info == "房屋年限":
                trade_key_info = "HouseYears"
            elif trade_key_info == "抵押信息":
                trade_key_info = "MortgageInfo"
            elif trade_key_info == "房屋用途":
                trade_key_info = "HouseUse"
            elif trade_key_info == "产权所属":
                trade_key_info = "PropertyBelong"
            elif trade_key_info == "房本备件":
                trade_key_info = "HouseCertificate"
            item1.fields[trade_key_info] = Field()
            item1[trade_key_info] = trade_value_info
        item1.update(item)
        if self.is_finished():
            self.do_final()
        yield item1

    def do_final(self):
        '''
        更新可售为已售、可租为已租
        :param cursor:
        :param spider:
        :return:
        '''
        housing_trade_list = [x for x in self.url_list if x not in self.scaned_url_list]
        logging.info(housing_trade_list)
        for housing_url in housing_trade_list:
            yield scrapy.Request(url=housing_url, callback=self.house_status_handle, headers=self.get_headers())

    def house_status_handle(self, response):
        # 验证码
        url = response.url
        item = {}
        flag = response.xpath("//div[@class='title']/h1[@class='main']/text()").extract_first()
        flag_shelves = response.xpath("//div[@class='title']/h1[@class='main']/span/text()").extract_first()
        if flag and flag_shelves == None:
            pass
        else:
            item["flag_remaining"] = True
            item["HouseUrl"] = url
            yield item

