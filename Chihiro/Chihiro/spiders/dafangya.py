# -*- coding: utf-8 -*-
import scrapy
import re
import logging
import json
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field
import datetime
import time
from lxml import etree
import jsonpath


class Chihiro(scrapy.Spider):
    name = 'dafangya'
    base_url = "https://www.dafangya.com"
    start_urls = [
        "https://www.dafangya.com/api/v3/search/list?level=11&ot=0&pnr=-1%7C-1&bnr=-1%7C-1&tnr=-1%7C-1&fr=-1%7C-1&bar=-1%7C-1&pdr=-1&pr=-1%7C-1&ar=-1%7C-1&dt=-1&hf=&hut=&ll=&sort=houseFrom%2Casc&sort=publishDate%2Cdesc&sort=auto&size=20&page=0&q=1&_=1582265995000&ele=&latL=30.805293&lonL=120.937155&latR=31.558611&lonR=121.831724"]
    PropertyCity = '上海'
    Resource = '大房鸭'
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
            'Accept': 'application/json; version=2.0; charset=utf-8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Connection': 'keep-alive', 不要保持长连接
            'Host': 'www.dafangya.com',
            'Referer': 'https://www.dafangya.com/initSearch.html?businessType=0',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'X-Requested-With': 'XMLHttpRequest',
            'yys': '0',
        },
        # "DOWNLOAD_DELAY": 0.3,
        # "CONCURRENT_REQUESTS": 1,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            # 'Chihiro.middleware_request.ChihiroDownloaderMiddleware': 543,
            # 'Chihiro.middleware_request.UserAgent_Middleware': 543,
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

    def get_home_data(self, obj):
        city_list = jsonpath.jsonpath(obj, '$..provinceName')
        area_list = jsonpath.jsonpath(obj, '$..districtName')
        road_list = jsonpath.jsonpath(obj, '$..plateName')
        house_url_list = jsonpath.jsonpath(obj, '$..shortLink')
        build_time_list = jsonpath.jsonpath(obj, '$..buildYear')
        release_time_list = jsonpath.jsonpath(obj, '$..publishDate')  # TODO 时间戳 准换为时间
        addr_list = jsonpath.jsonpath(obj, '$..address')
        # floor_list = jsonpath.jsonpath(obj, '$..floor')
        total_price_list = jsonpath.jsonpath(obj, '$..price')
        community_list = jsonpath.jsonpath(obj, '$..neighborhoodName')
        build_size_list = jsonpath.jsonpath(obj, '$..area')
        elevator_list = jsonpath.jsonpath(obj, '$..elevator')
        offlineDate_list = jsonpath.jsonpath(obj, '$..offlineDate')
        data = {'city': city_list, 'area': area_list,
                'road': road_list, 'house_url': house_url_list, 'build_time': build_time_list,
                'release_time': release_time_list, 'addr': addr_list,
                'total_price': total_price_list,
                'build_size': build_size_list, 'community': community_list,
                'elevator': elevator_list, 'resource_status': offlineDate_list,
                }
        return data

    # 获取次页数据
    def get_plate_data(self, tree, item):
        try:
            door_model = tree.xpath('//span[contains(@class,"margin-left24")]/text()')[0].strip().split('（')[0]
            house_use = tree.xpath('//span[@class="condition condition1"]/text()')[0].strip()
            direction = tree.xpath('//span[@class="condition condition2"]/text()')[0].strip()
            decoration = tree.xpath('//span[@class="condition condition4"]/text()')[0].strip()
            per_price = tree.xpath('//span[@class="font-normal font-white font-size14 margin-left10"]/text()')[
                0].strip().split(
                '￥')[-1]
            floor_handle = tree.xpath('//span[@id="house_floor_total_span"]/text()')[0].strip()
            total_floor_handle = re.search(" / (.*?)（", floor_handle)
            if total_floor_handle:
                total_floor = total_floor_handle.group(1)
            else:
                total_floor = re.search(" 共(.*?)层", floor_handle)
                if total_floor:
                    total_floor = total_floor.group(1)
                else:
                    total_floor = None
            floor = re.search("(.*)\s+/", floor_handle)
            if floor:
                floor = floor.group(1).strip()
            else:
                floor = re.search("(.*)\s+\(", floor_handle)
                if floor:
                    floor = floor.group(1)
                else:
                    floor = None
            # print('===================', total_floor)
            item.fields["HouseType"] = Field()
            item.fields["TotalFloor"] = Field()
            item.fields["Floor"] = Field()
            item.fields["HouseUse"] = Field()
            item.fields["HouseDirection"] = Field()
            item.fields["FixTypeName"] = Field()
            item.fields["PriceUnit"] = Field()
            item['HouseType'] = door_model
            item['TotalFloor'] = total_floor or None
            item['Floor'] = floor
            item['HouseUse'] = house_use
            item['HouseDirection'] = direction
            item['FixTypeName'] = decoration
            item['PriceUnit'] = per_price
            return item
        except Exception as e:
            print("item异常：{}".format(e))
            logging.info("item异常：{}".format(e))

    def parse(self, response):
        base_url = 'https://www.dafangya.com/api/v2/search/list?level=12&ot=0&pnr=-1%7C-1&bnr=-1%7C-1&tnr=-1%7C-1&fr=-1%7C-1&bar=-1%7C-1&pdr=-1&pr=-1%7C-1&ar=-1%7C-1&dt=-1&hf=&hut=&ll=&sort=houseFrom%2Casc&sort=publishDate%2Cdesc&sort=auto&size=1000&page={}&q=1&ele=&latL=31.043706&lonL=121.376391&latR=31.408334&lonR=121.564963'
        # 总套数
        total_num = json.loads(response.text).get("totalElements")
        total_page = int(total_num) // 1000
        for i in range(total_page):
            next_url = base_url.format(i)
            yield scrapy.Request(url=next_url, callback=self.second_parse)

    def second_parse(self, response):
        obj = json.loads(response.text)
        data = self.get_home_data(obj)
        for index, url in enumerate(data['house_url']):
            # TODO 新增判断网页重复
            item = Item()
            item.fields["AreaName"] = Field()
            item.fields["PlateName"] = Field()
            item.fields["HouseUrl"] = Field()
            item.fields["BuildedTime"] = Field()
            item.fields["TimeToRelease"] = Field()
            item.fields["PropertyAddress"] = Field()
            # item.fields["Floor"] = Field()
            item.fields["TotalPrice"] = Field()
            item.fields["BuildingSquare"] = Field()
            item.fields["PropertyCommunity"] = Field()
            item.fields["HasElevator"] = Field()

            item['AreaName'] = data['area'][index]
            item['PlateName'] = data['road'][index]
            flag = data['house_url']
            if flag:
                house_url = flag[index]
                if house_url:
                    item['HouseUrl'] = self.base_url + house_url
                    item['BuildedTime'] = data['build_time'][index]
                    item['TimeToRelease'] = time.strftime("%Y-%m-%d",
                                                          time.localtime(int(str(data['release_time'][index])[:-3])))
                    item['PropertyAddress'] = data['addr'][index]
                    # item['Floor'] = data['floor'][index]
                    item['TotalPrice'] = data['total_price'][index]
                    item['BuildingSquare'] = data['build_size'][index]
                    item['PropertyCommunity'] = data['community'][index]
                    item['HasElevator'] = data['elevator'][index]
                    try:
                        next_url = "https://www.dafangya.com" + url
                    except:
                        continue
                    yield scrapy.Request(url=next_url,
                                         callback=self.third_parse,
                                         meta={'item': item}, dont_filter=True)

    def third_parse(self, response):
        if response.status == 200:
            tree = etree.HTML(response.text)
            item = response.meta['item']
            # 获取次页数据
            item = self.get_plate_data(tree, item)
            if item != None:
                yield item