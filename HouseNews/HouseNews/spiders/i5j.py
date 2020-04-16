# -*- coding: utf-8 -*-
import scrapy
import re
import math
import pymssql
import random
import time
import pandas as pd
from sqlalchemy import create_engine
from scrapy.item import Field, Item
import requests
from selenium import webdriver
import sys


class I5jSpider(scrapy.Spider):
    name = 'i5j'
    custom_settings = {
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
        "BOT_NAME": 'i5j',
        "RETRY_HTTP_CODES": [403, 302, 502],
        "RETRY_TIMES": 10,
        'ITEM_PIPELINES': {
            'HouseNews.pipelines.HousenewsPipeline': 100,
        },
        'DOWNLOADER_MIDDLEWARES': {
            # 'HouseNews.middlewares.HousenewsDownloaderMiddleware': 543,
        },
        'COOKIES_ENABLED': False,
        # "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 20,
        # "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        # "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        # "SCHEDULER_PERSIST": True,
        # "REDIS_HOST": "127.0.0.1",
        # "REDIS_PORT": 6379,
        # "REDIS_PASSWORD": 6379,
    }

    def __init__(self):
        # self.ershoufang_url = "https://m.5i5j.com/sh/ershoufang/"
        # self.zufang_url = "https://m.5i5j.com/sh/zufang/"
        self.base_headers = self.custom_settings.get("DEFAULT_REQUEST_HEADERS")
        self.base_url = "https://sh.5i5j.com"
        self.ershoufang_start_url = "https://sh.5i5j.com/ershoufang/"
        self.zufang_start_url = "https://sh.5i5j.com/zufang/"
        self.city = "上海"
        self.source = "我爱我家"
        self.engine_third_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWSpider')
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where Resource='%s'" % self.source,
            self.engine_third_house)
        self.url_status_list = self.sql_select.values
        self.url_status_list_available = []
        self.url_list = self.sql_select["HouseUrl"].to_list()
        self.headers = [
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
            'Opera/9.25 (Windows NT 5.1; U; en)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
        ]
        self.current_path = sys.path[0]
        self.browser = webdriver.Chrome(executable_path="{}\chromedriver.exe".format(self.current_path))
        self.browser.get("https://sh.5i5j.com/ershoufang/")
        self.cookies = self.browser.get_cookies()
        self.cookies_str = ""
        for i in self.cookies:
            k = i.get("name")
            v = i.get("value")
            self.cookies_str += k + "=" + v + ";"
        print(self.cookies_str)
        self.browser.quit()
        self.cookies = self.cookies_str

    def handle_cookies(self):
        browser = webdriver.Chrome(executable_path="D:\My project\TW\spider\Chihiro\\test_code\chromedriver.exe")
        browser.get("https://sh.5i5j.com/ershoufang/")
        cookies = browser.get_cookies()
        cookies_str = ""
        for i in cookies:
            k = i.get("name")
            v = i.get("value")
            cookies_str += k + "=" + v + ";"
        print(cookies_str)
        browser.quit()
        return cookies_str

    def handle_headers(self):
        headers = {
            'User-Agent': random.choice(self.headers),
            'Cookie': self.cookies
        }
        headers.update(self.base_headers)
        return headers

    def start_requests(self):
        yield scrapy.Request(url=self.ershoufang_start_url, callback=self.region_handle,
                             meta={"ershoufang_start_url": True},
                             headers=self.handle_headers(), dont_filter=True)
        yield scrapy.Request(url=self.zufang_start_url, callback=self.region_handle,
                             headers=self.handle_headers(), dont_filter=True)

    def region_handle(self, response):
        region_xpath_list = response.xpath(
            "//ul[contains(@class,'new_di_tab')]/a[position()>1]")
        ershoufang_start_url = response.meta.get("ershoufang_start_url")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./li/text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle,
                                 meta={"region": region, "ershoufang_start_url": ershoufang_start_url},
                                 headers=self.handle_headers())

    def plate_handle(self, response):
        region = response.meta.get("region")
        ershoufang_start_url = response.meta.get("ershoufang_start_url")
        plate_xpath_list = response.xpath(
            "//div[contains(@class,'block')]/dl[contains(@class,'quyuCon')]/dd[@class='block']/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            plate_url = self.base_url + plate_url_handle
            yield scrapy.Request(url=plate_url, callback=self.parse,
                                 meta={"region": region, "plate": plate, "ershoufang_start_url": ershoufang_start_url},
                                 headers=self.handle_headers())

    def parse(self, response):
        ershoufang_start_url = response.meta.get("ershoufang_start_url")
        housing_list = response.xpath("//div[@class='list-con-box'][1]/ul[@class='pList']/li/div[@class='listCon']")
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        for housing in housing_list:
            item = Item()
            item.fields["区域"] = Field()
            item["区域"] = region
            item.fields["板块"] = Field()
            item["板块"] = plate
            housing_url = housing.xpath("./h3/a/@href").extract_first()
            housing_url = self.base_url + housing_url
            housing_description = housing.xpath("./h3/a/text()").extract_first()
            housing_info = housing.xpath("./div[@class='listX']/p[1]/text()").extract_first().replace(" ", '')
            housing_info = housing_info.split("·")
            # 二手房为总价；租房为单价
            info_red = housing.xpath(
                "./div[@class='listX']/div[@class='jia']/p[@class='redC']/strong/text()").extract_first()
            info_grey = housing.xpath("./div[@class='listX']/div[@class='jia']/p[2]/text()").extract_first()
            item.fields["总价"] = Field()
            item.fields["单价"] = Field()
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
            item.fields["房源描述"] = Field()
            item["房源描述"] = housing_description
            item.fields["房源链接"] = Field()
            item["房源链接"] = housing_url
            item.fields["城市"] = Field()
            item["城市"] = self.city
            item.fields["来源"] = Field()
            item["来源"] = self.source
            item.fields["租售状态"] = Field()
            # 房屋户型
            item.fields["房屋户型"] = Field()
            item["房屋户型"] = room_type
            # 建筑面积
            item.fields["建筑面积"] = Field()
            item["建筑面积"] = room_area
            # 房屋朝向
            item.fields["房屋朝向"] = Field()
            item["房屋朝向"] = room_orientation
            # 总楼层
            item.fields["总楼层"] = Field()
            item["总楼层"] = total_floor
            # 所在楼层
            item.fields["所在楼层"] = Field()
            item["所在楼层"] = location_floor
            # 装修情况
            item.fields["装修情况"] = Field()
            item["装修情况"] = room_decorate
            # 建成年份
            item.fields["建成年份"] = Field()
            item["建成年份"] = build_year
            # 小区
            community = housing.xpath("./div[@class='listX']/p[2]/a[1]/text()").extract_first()
            # 地址
            address = housing.xpath("./div[@class='listX']/p[2]/a[2]/text()").extract_first()
            if address:
                address = address.replace(" ", '')
            item.fields["小区"] = Field()
            item["小区"] = community
            item.fields["地址"] = Field()
            item["地址"] = address

            if ershoufang_start_url:
                item["租售状态"] = 0
                item["总价"] = info_red
                item["单价"] = info_grey.replace("单价", "")
                item.fields["房源状态"] = Field()
                item["房源状态"] = "可售"
                flag = [housing_url, "可售"]
                if housing_url in self.url_list:
                    if flag in self.url_status_list:
                        self.url_status_list_available.append(flag)
                    else:
                        # 更新已租为可租
                        item.fields["flag_item"] = Field()
                        item["flag_item"] = 1
                        self.url_status_list_available.append([housing_url, '已售'])
                        yield item
                else:
                    # 新增
                    yield item
            else:
                item["租售状态"] = 1
                item.fields["租赁方式"] = Field()
                item.fields["房源状态"] = Field()
                item["房源状态"] = "可租"

                item["总价"] = None
                item["单价"] = info_red
                if info_grey:
                    rental_way = info_grey.replace("出租方式：", "")
                    item["租赁方式"] = rental_way
                flag = [housing_url, "可租"]
                if housing_url in self.url_list:
                    if flag in self.url_status_list:
                        self.url_status_list_available.append(flag)
                    else:
                        # 更新已租为可租
                        item.fields["flag_item"] = Field()
                        item["flag_item"] = 1
                        self.url_status_list_available.append([housing_url, '已租'])
                        yield item
                else:
                    # 新增
                    yield item
        next_page_handle = response.xpath("//a[@class='cPage'][1]/@href").extract_first()
        if next_page_handle:
            next_page = self.base_url + next_page_handle
            yield scrapy.Request(url=next_page, callback=self.parse,
                                 meta={"ershoufang_start_url": ershoufang_start_url, "region": region, "plate": plate},
                                 headers=self.handle_headers())
