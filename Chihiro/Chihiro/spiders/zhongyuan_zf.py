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
    name = 'zhongyuan_zf'
    base_url = "https://sh.centanet.com"
    start_urls = ['https://sh.centanet.com/zufang/']
    PropertyCity = '上海'
    Resource = '中原'
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
            'Accept-Encoding': '*/*',
            'accept-language': 'gzip',
            'content-type': 'charset=utf8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'Cookie': "gr_user_id=8cfc279d-37f1-4214-a9c4-3bc18729c385; grwng_uid=96cd6356-a60d-4b6b-ab22-361f1d1d839a; Y190cmFja2lk=73a9977002ec49b68dcc0fdde38ccc17; acw_tc=65597d1615705879540916094ee97fc1cd6e308264fee86e78dba6a105; Hm_lvt_219872fb6de637cac5884769682da5ad=1570587955,1571127896,1572328984; gioClientCookie=d29e91b3-d697-4177-a822-38ffe0ab608d; _pk_ref.10.5e68=%5B%22%22%2C%22%22%2C1572418167%2C%22http%3A%2F%2Fdefault.centanet.com%2F%22%5D; _pk_ses.10.5e68=*;_pk_id.10.5e68=91dbe8d1a8824461.1570587955.18.1572419485.1572328990.; dft034f=Wu%2FMiTlp99s0GdWA9dWmKg__; Hm_lpvt_219872fb6de637cac5884769682da5ad=1572419485",
        },
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 1,
        "RETRY_HTTP_CODES": [302, 403, 502],
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
            "//ul[contains(@class,'tap_show')]/li/a")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle, meta={"region": region},
                                 headers=self.get_headers())

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//div[@class='tagbox_wrapper_main']/div[@class='tagbox_wrapper_cd']/ul[contains(@class,'tap_show')]/li[position()>1]/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            plate_url = self.base_url + plate_url_handle
            yield scrapy.Request(url=plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate, "plate_url": plate_url},
                                 headers=self.get_headers())

    def page_handle(self, response):
        plate_url = response.meta.get("plate_url")
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        housing_list = response.xpath(
            "//div[@class='wrap-content']/ul[@id='ShowStyleByTable']/li/div[contains(@class,'wp-ct-box')]")
        next_page_handle = response.xpath("//a[contains(@class,'clickmore')]/@href").extract_first()
        for housing in housing_list:
            # 列表页面字段获取
            item = Item()
            item.fields["AreaName"] = Field()
            item["AreaName"] = region
            item.fields["PlateName"] = Field()
            item["PlateName"] = plate
            housing_description = housing.xpath(
                "./div[@class='ct-box-c']/div[@class='box-c-tt']/a/text()").extract_first()
            housing_url = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-tt']/a/@href").extract_first()
            housing_url = self.base_url + housing_url
            item.fields["HouseDesc"] = Field()
            item["HouseDesc"] = housing_description
            item.fields["HouseUrl"] = Field()
            item["HouseUrl"] = housing_url
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
            item.fields["PropertyCommunity"] = Field()
            item["PropertyCommunity"] = community
            item.fields["PropertyAddress"] = Field()
            item["PropertyAddress"] = address
            item.fields["PriceUnit"] = Field()
            item["PriceUnit"] = unit_price
            item.fields["TotalPrice"] = Field()
            item["TotalPrice"] = total_price
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
                item.fields["HouseType"] = Field()
                item["HouseType"] = room_type
                item.fields["BuildingSquare"] = Field()
                item["BuildingSquare"] = room_area
                item.fields["HouseDirection"] = Field()
                item["HouseDirection"] = room_orientation
                item.fields["TotalFloor"] = Field()
                item["TotalFloor"] = total_floor
                item.fields["Floor"] = Field()
                item["Floor"] = location_floor
                item.fields["FixTypeName"] = Field()
                item["FixTypeName"] = room_decorate
                item.fields["BuildedTime"] = Field()
                item["BuildedTime"] = build_year
            yield item
        if next_page_handle:
            next_page = self.base_url + next_page_handle
            yield scrapy.Request(url=next_page, callback=self.page_handle,
                                 meta={"plate_url": plate_url, 'region': region,
                                       "plate": plate}, headers=self.get_headers())

