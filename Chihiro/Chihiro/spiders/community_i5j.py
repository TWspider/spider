# -*- coding: utf-8 -*-
import scrapy
import re
import os
import json
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field
from selenium import webdriver
import sys


class Chihiro(scrapy.Spider):
    name = 'community_i5j'
    base_url = "https://sh.5i5j.com"
    start_urls = ['https://sh.5i5j.com/xiaoqu/']
    PropertyCity = '上海'
    Resource = '我爱我家'
    custom_settings = {
        # 基本参数
        "BOT_NAME": name,
        "PropertyCity": PropertyCity,
        "Resource": Resource,
        # 请求参数
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'cookie': '',
            # 'Cache-Control': 'max-age=0',
            # 'Connection': 'keep-alive',
            # 'Host': 'sh.5i5j.com',
            # 'Referer': 'https://sh.5i5j.com/ershoufang/',
            # 'Sec-Fetch-Mode': 'navigate',
            # 'Sec-Fetch-Site': 'same-origin',
            # 'Sec-Fetch-User': '?1',
            # 'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        },
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 5,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            # 'Chihiro.middleware_request.ChihiroDownloaderMiddleware': 543,
            'Chihiro.middleware_request.UserAgent_Middleware': 500,
            # 'Chihiro.middleware_request.ChromeDownloaderMiddleware': None,
            'Chihiro.middleware_request.IpAgent_Middleware': 222,
            'Chihiro.middleware_request.CookiesClear': 333,
            # 'Chihiro.middleware_request.RequestsMiddleware': 222,
        },
        # 清洗参数
        "SPIDER_MIDDLEWARES": {
            # 'Chihiro.middleware_item.ChihiroSpiderMiddleware': 500,
        },
        # 业务参数
        "ITEM_PIPELINES": {
            'Chihiro.middleware_sql.CommunityPipeline': 300,
        },
        # 错误记录
        # ERROR_RECORD = True
        # 日志
        # "LOG_LEVEL": 'INFO',
        # "LOG_FILE": "Community.txt",
        # True：使用浏览器返回的cookie
        # False：自己设置cookie
        "COOKIES_ENABLED": True,
        "COOKIES_DEBUG": True
        # "REDIRECT_ENABLED": False
    }

    def parse(self, response):

        region_xpath_list = response.xpath(
            "//ul[contains(@class,'new_di_tab')]/a[position()>1]")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./li/text()").extract_first()
            region = re.sub("\s+", '', region).strip()
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
            plate = re.sub("\s+", '', plate).strip()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            plate_url = self.base_url + plate_url_handle
            yield scrapy.Request(url=plate_url, callback=self.house_handle,
                                 meta={"region": region, "plate": plate, "plate_url": plate_url})

    def house_handle(self, response):
        housing_list = response.xpath("//div[@class='list-con-box'][1]/ul[@class='pList']/li/div[@class='listCon']")
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        plate_url = response.meta.get("plate_url")
        for housing in housing_list:
            item = Item()
            item.fields["AreaName"] = Field()
            item["AreaName"] = region
            item.fields["PlateName"] = Field()
            item["PlateName"] = plate
            housing_url = housing.xpath("./h3/a/@href").extract_first()
            CommunityUrl = self.base_url + housing_url
            PropertyCommunity = housing.xpath("./h3/a/text()").extract_first()
            item.fields["CommunityUrl"] = Field()
            item["CommunityUrl"] = CommunityUrl
            item.fields["PropertyCommunity"] = Field()
            item["PropertyCommunity"] = PropertyCommunity
            yield scrapy.Request(url=CommunityUrl, callback=self.handle_1,
                                 meta={"item": deepcopy(item)})

        current_page = response.xpath("//div[@class='pageBox']/div/a[@class='cur']/text()").extract_first()
        total_page = response.xpath("//div[contains(@class,'total-box')]/span/text()").extract_first()
        if total_page and current_page:
            total_page = int(total_page) // 20 + 1
            current_page = int(current_page)
            if current_page < total_page:
                next_page = plate_url + "n" + str(current_page + 1) + "/"
                yield scrapy.Request(url=next_page, callback=self.house_handle,
                                     meta={"plate_url": plate_url, 'region': region,
                                           "plate": plate}
                                     )

    def handle_1(self, response):
        item = response.meta.get("item")
        item1 = Item()
        PropertyAddress = response.xpath(
            "//div[contains(@class,'rent-top')]/a/text()").extract_first()
        PriceUnit = response.xpath(
            "//div[contains(@class,'junjia')]/span/text()").extract_first()
        ls_detail = response.xpath("//div[@class='xqfangs detail_bor_bottom']/ul[@class='clear']/li/text()").extract()
        BuildedTime = None
        BuildingType = None
        for detail in ls_detail:
            if "年" in detail:
                BuildedTime = detail
            else:
                BuildingType = detail
        PropertyCompany = response.xpath(
            "//ul/li[@class='wuyes']/em/text()").extract_first()
        Developers = response.xpath(
            "//ul/li[@class='kaifas']/em/text()").extract_first()
        TotalBuilding = response.xpath(
            "//div[@class='xqsaleinfo']/ul/li[1]/span/text()").extract_first()
        TotalHouseholds = response.xpath(
            "//div[@class='xqsaleinfo']/ul/li[2]/span/text()").extract_first()
        NearbyStores = response.xpath(
            "//div[@class='xqsaleinfo']/ul/li[6]/span/text()").extract_first()
        AroundTraffic = response.xpath("//div[@class='xqsaleinfo']/ul/li[5]/span/text()").extract_first()
        item1.fields["PropertyAddress"] = Field()
        item1["PropertyAddress"] = PropertyAddress
        item1.fields["PriceUnit"] = Field()
        item1["PriceUnit"] = PriceUnit

        item1.fields["BuildedTime"] = Field()
        item1["BuildedTime"] = BuildedTime
        item1.fields["BuildingType"] = Field()
        item1["BuildingType"] = BuildingType
        item1.fields["PropertyCompany"] = Field()
        item1["PropertyCompany"] = PropertyCompany
        item1.fields["Developers"] = Field()
        item1["Developers"] = Developers
        item1.fields["TotalBuilding"] = Field()
        item1["TotalBuilding"] = TotalBuilding
        item1.fields["TotalHouseholds"] = Field()
        item1["TotalHouseholds"] = TotalHouseholds
        item1.fields["NearbyStores"] = Field()
        item1["NearbyStores"] = NearbyStores
        item1.fields["AroundTraffic"] = Field()
        item1["AroundTraffic"] = AroundTraffic
        item1.update(item)
        yield item1
