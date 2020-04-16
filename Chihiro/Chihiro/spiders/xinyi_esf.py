# -*- coding: utf-8 -*-
import datetime
import math
from scrapy import Item, Field
import pymssql
import scrapy
import re
import json
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem


class Chihiro(scrapy.Spider):
    name = 'xinyi_esf'
    start_urls = ['http://www.sinyi.com.cn/handhouse/list/']
    PropertyCity = '上海'
    Resource = '信义房屋'
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

    def parse(self, response):
        total_page = math.ceil(int(response.xpath('//*[@id="listCover"]/div/div[1]/span/text()').extract_first()) / 10)
        for page in range(1, total_page + 1):
            page_url = 'http://www.sinyi.com.cn/handhouse/list?page={}&pagesize=10'.format(page)
            yield scrapy.Request(page_url, callback=self.second_parse)

    def second_parse(self, response):
        href_list = response.xpath('//ul[@class="wp100"]/li//div/p[@class="fs22"]/a/@href')
        print(len(href_list), response.url)
        for href in href_list:
            item = Item()
            next_url = 'http://www.sinyi.com.cn/' + href.extract().split('/', 1)[1].split('&cookieuid=')[0]
            item.fields['HouseUrl'] = Field()
            item['HouseUrl'] = next_url
            yield scrapy.Request(next_url, callback=self.third_parse, meta={'item': item})

    def third_parse(self, response):
        item = response.meta['item']
        item.fields['HouseDesc'] = Field()
        item.fields['TotalPrice'] = Field()
        item.fields['HouseType'] = Field()
        item.fields['BuildingSquare'] = Field()
        item.fields['PubCompany'] = Field()
        item.fields['Agent'] = Field()
        item.fields['PropertyCommunity'] = Field()
        item.fields['PropertyAddress'] = Field()
        item.fields['PriceUnit'] = Field()
        item.fields['TotalFloor'] = Field()
        item.fields['Floor'] = Field()
        item.fields['AreaName'] = Field()
        item.fields['HouseDirection'] = Field()

        item['HouseDesc'] = response.xpath(
            '/html/body/section[2]/section[1]/div[1]/p[1]/span[1]/text()').extract_first().strip()
        item['TotalPrice'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/p/span[1]/text()').extract_first().strip()
        item['HouseType'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/div/div[1]/p[2]/text()').extract_first().strip()
        item['BuildingSquare'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/div/div[2]/p[2]/text()').extract_first().strip()

        item['PubCompany'] = '信义房屋'
        item['Agent'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[2]/div[1]/div/p[1]/a/span/text()').extract_first().strip()
        item['PropertyCommunity'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/ul/li[6]/a/span/text()').extract_first().strip()
        item['PropertyAddress'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/ul/li[7]/a/text()').extract_first().strip().split(
            ']')[1]
        item['PriceUnit'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/p/span[3]/text()').extract_first().strip()
        floor_desc = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/ul/li[4]/span[2]/text()').extract_first().strip()
        item['TotalFloor'] = re.findall('(\d+)层', floor_desc)[0]
        item['Floor'] = floor_desc.split('/')[0]
        item['AreaName'] = response.xpath(
            '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/ul/li[7]/a/text()').extract_first().strip().split(
            ' ')[0][1:]
        try:
            item['HouseDirection'] = response.xpath(
                '/html/body/section[2]/section[1]/div[2]/div[2]/div[1]/div[2]/ul/liv/span[2]/text()').extract_first().strip()
        except:
            item['HouseDirection'] = 0
        yield item
