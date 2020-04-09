# -*- coding: utf-8 -*-
import scrapy
import re
import json
from scrapy.item import Field, Item
import random
import pandas as pd
import pymssql
from sqlalchemy import create_engine


class ZhongyuanSpider(scrapy.Spider):
    name = 'zhongyuan'
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
            'content-type': 'charset=utf8',
            'Cookie': "gr_user_id=8cfc279d-37f1-4214-a9c4-3bc18729c385; grwng_uid=96cd6356-a60d-4b6b-ab22-361f1d1d839a; Y190cmFja2lk=73a9977002ec49b68dcc0fdde38ccc17; acw_tc=65597d1615705879540916094ee97fc1cd6e308264fee86e78dba6a105; Hm_lvt_219872fb6de637cac5884769682da5ad=1570587955,1571127896,1572328984; gioClientCookie=d29e91b3-d697-4177-a822-38ffe0ab608d; _pk_ref.10.5e68=%5B%22%22%2C%22%22%2C1572418167%2C%22http%3A%2F%2Fdefault.centanet.com%2F%22%5D; _pk_ses.10.5e68=*;_pk_id.10.5e68=91dbe8d1a8824461.1570587955.18.1572419485.1572328990.; dft034f=Wu%2FMiTlp99s0GdWA9dWmKg__; Hm_lpvt_219872fb6de637cac5884769682da5ad=1572419485",
        },
        'ITEM_PIPELINES': {
            # 'HouseNews.pipelines.HousenewsPipeline': 100,
        },
        # "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        # "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        # "SCHEDULER_PERSIST": True,
        # "REDIS_HOST": "127.0.0.1",
        # "REDIS_PORT": 6379,
        # "REDIS_PASSWORD": 6379,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 10,
        "RETRY_HTTP_CODES": [403, 302, 502],
        "RETRY_TIMES": 10,
        "BOT_NAME": 'zhongyuan',
    }

    def __init__(self):
        self.base_url = "https://sh.centanet.com"
        self.start_url = "https://sh.centanet.com/ershoufang/"
        self.city = "上海"
        self.source = "中原"
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

    def start_requests(self):
        # 选择城市
        yield scrapy.Request(url=self.start_url, callback=self.parse,
                             headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)
        # start_url = "https://bj.centanet.com/ershoufang/dinghuisi/"
        # yield scrapy.Request(url=start_url, callback=self.page_handle)

    def parse(self, response):
        region_xpath_list = response.xpath(
            "//ul[contains(@class,'tap_show')]/li/a")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle, meta={"region": region},
                                 headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//div[@class='tagbox_wrapper_main']/div[@class='tagbox_wrapper_cd']/ul[contains(@class,'tap_show')]/li[position()>1]/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            ershoufang_plate_url = self.base_url + plate_url_handle
            zufang_plate_url = self.base_url + plate_url_handle.replace("ershoufang", "zufang")
            yield scrapy.Request(url=ershoufang_plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate, "plate_url": ershoufang_plate_url},
                                 headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)
            yield scrapy.Request(url=zufang_plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate},
                                 headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)

    def page_handle(self, response):
        plate_url = response.meta.get("plate_url")
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        # ershoufang_plate_url = "https://sh.lianjia.com/ershoufang/andingmen/"
        housing_list = response.xpath(
            "//div[@class='wrap-content']/ul[@id='ShowStyleByTable']/li/div[contains(@class,'wp-ct-box')]")
        next_page_handle = response.xpath("//a[contains(@class,'clickmore')]/@href").extract_first()
        for housing in housing_list:
            # 列表页面字段获取
            item = Item()
            item.fields["城市"] = Field()
            item["城市"] = self.city
            item.fields["来源"] = Field()
            item["来源"] = self.source
            item.fields["区域"] = Field()
            item["区域"] = region
            item.fields["板块"] = Field()
            item["板块"] = plate
            housing_description = housing.xpath(
                "./div[@class='ct-box-c']/div[@class='box-c-tt']/a/text()").extract_first()
            housing_url = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-tt']/a/@href").extract_first()
            housing_url = self.base_url + housing_url
            item.fields["房源描述"] = Field()
            item["房源描述"] = housing_description
            item.fields["房源链接"] = Field()
            item["房源链接"] = housing_url
            # 小区
            community = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-tp']/a/text()").extract_first()
            # 地址
            address = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-lc']/p/text()").extract_first()
            housing_info = housing.xpath(
                "./div[@class='ct-box-c']/div[@class='box-c-tp']").xpath(
                "string(.)").extract_first()
            housing_info_handle = housing_info.replace(community, '').replace(' ', '').replace("\n", '')
            housing_info_handle_two = housing_info_handle.split("|")
            # 单价
            unit_price = housing.xpath("./div[@class='ct-box-r']/p/text()").extract_first()
            # 总价
            total_price = housing.xpath("./div[@class='ct-box-r']/h3/span/text()").extract_first()
            item.fields["小区"] = Field()
            item["小区"] = community
            item.fields["地址"] = Field()
            item["地址"] = address
            item.fields["单价"] = Field()
            item["单价"] = unit_price
            item.fields["总价"] = Field()
            item["总价"] = total_price
            if housing_info_handle_two:
                # 房屋户型
                room_type = housing_info_handle_two[0]
                # 建筑面积
                room_area = housing_info_handle_two[1]
                # 房屋朝向
                room_orientation = housing_info_handle_two[2]
                # 所在楼层
                location_floor = housing_info_handle_two[3]
                # 总楼层
                total_floor = housing_info_handle_two[4]
                # 装修情况
                room_decorate = housing_info_handle_two[5]
                # 建成年份
                build_year = housing_info_handle_two[6]
                item.fields["房屋户型"] = Field()
                item["房屋户型"] = room_type
                item.fields["建筑面积"] = Field()
                item["建筑面积"] = room_area
                item.fields["房屋朝向"] = Field()
                item["房屋朝向"] = room_orientation
                item.fields["总楼层"] = Field()
                item["总楼层"] = total_floor
                item.fields["所在楼层"] = Field()
                item["所在楼层"] = location_floor
                item.fields["装修情况"] = Field()
                item["装修情况"] = room_decorate
                item.fields["建成年份"] = Field()
                item["建成年份"] = build_year
            if plate_url:
                item.fields["房源状态"] = Field()
                item["房源状态"] = "可售"
                item.fields["租售状态"] = Field()
                item["租售状态"] = 0
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
                item.fields["房源状态"] = Field()
                item["房源状态"] = "可租"
                item.fields["租售状态"] = Field()
                item["租售状态"] = 1
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
        if next_page_handle:
            next_page = self.base_url + next_page_handle
            yield scrapy.Request(url=next_page, callback=self.page_handle,
                                 meta={"plate_url": plate_url, 'region': region,
                                       "plate": plate}, headers={"User-Agent": random.choice(self.headers)})

    # def housing_handle(self, response):
    #     item = response.meta.get("item")
    #     租售状态 = item.get("租售状态")
    #     item1 = Item()
    #     # 二手房
    #     if not 租售状态:
    #         pass
    #
    #     # 租房
    #     else:
    #         pass
    #     item1.update(item)
    #     yield item1
