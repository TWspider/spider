# -*- coding: utf-8 -*-
import scrapy
import copy
import json
import re
from scrapy.item import Field, Item
import random
import pandas as pd
import pymssql
from sqlalchemy import create_engine


# item.fields["项目链接"] = Field()
# item["项目链接"] = housing_detail_link

class ScanLianjiaSpider(scrapy.Spider):
    name = 'lianjia'
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'
        },
        'ITEM_PIPELINES': {
            'HouseNews.pipelines.HousenewsPipeline': 100,
        },
        # "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        # "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        # "SCHEDULER_PERSIST": True,
        # "REDIS_HOST": "127.0.0.1",
        # "REDIS_PORT": 6379,
        # "REDIS_PASSWORD": 6379,
        "RETRY_HTTP_CODES": [403, 302, 502],
        "RETRY_TIMES": 10,
        "BOT_NAME": 'lianjia',
    }

    def __init__(self):
        self.base_url = "https://sh.lianjia.com"
        self.start_url = "https://sh.lianjia.com/ershoufang/"
        self.city = "上海"
        self.source = "链家"
        self.engine_third_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWSpider')
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where Resource='%s'" % self.source,
            self.engine_third_house)
        self.url_list = self.sql_select["HouseUrl"].to_list()
        self.url_status_list = self.sql_select.values
        self.url_status_list_available = []
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

    def start_requests(self):
        # 选择城市
        yield scrapy.Request(url=self.start_url, callback=self.parse,
                             headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)

    #     start_url = "https://sh.lianjia.com/ershoufang/andingmen/"
    #     yield scrapy.Request(url=start_url, callback=self.test)
    #
    # def test(self, response):
    #     print(response)

    def parse(self, response):
        region_xpath_list = response.xpath(
            "//div[3]/div[@class='m-filter']/div[@class='position']/dl[2]/dd/div[1]/div/a")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle, meta={"region": region},
                                 headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//dl[2]/dd/div[1]/div[2]/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            ershoufang_plate_url = self.base_url + plate_url_handle
            zufang_plate_url = self.base_url + plate_url_handle.replace("ershoufang", "zufang")
            yield scrapy.Request(url=ershoufang_plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate, "ershoufang_plate_url": ershoufang_plate_url},
                                 headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)
            yield scrapy.Request(url=zufang_plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate, "zufang_plate_url": zufang_plate_url},
                                 headers={"User-Agent": random.choice(self.headers)}, dont_filter=True)

    def page_handle(self, response):
        ershoufang_plate_url = response.meta.get("ershoufang_plate_url")
        zufang_plate_url = response.meta.get("zufang_plate_url")
        region = response.meta.get("region")
        plate = response.meta.get("plate")
        if ershoufang_plate_url:
            housing_num_flag = response.xpath("//h2[contains(@class,'total')]/span/text()").extract_first().strip()
            if housing_num_flag != "0":
                housing_list = response.xpath(
                    "//div[@class='leftContent']/ul[@class='sellListContent']/li[contains(@class, 'clear')]/div[@class='info clear']")
                for housing in housing_list:
                    # 列表页面字段获取
                    item = Item()
                    item.fields["城市"] = Field()
                    item["城市"] = self.city
                    item.fields["来源"] = Field()
                    item["来源"] = self.source
                    item.fields["租售状态"] = Field()
                    item["租售状态"] = 0
                    item.fields["区域"] = Field()
                    item["区域"] = region
                    item.fields["板块"] = Field()
                    item["板块"] = plate
                    housing_description = housing.xpath("./div[@class='title']/a/text()").extract_first()
                    housing_url = housing.xpath("./div[@class='title']/a/@href").extract_first()
                    item.fields["房源描述"] = Field()
                    item["房源描述"] = housing_description
                    item.fields["房源链接"] = Field()
                    item["房源链接"] = housing_url
                    item.fields["房源状态"] = Field()
                    item["房源状态"] = "可售"
                    flag = [housing_url, '可售']
                    # 判断库内是否存在
                    if housing_url in self.url_list:
                        if flag in self.url_status_list:
                            self.url_status_list_available.append(flag)
                        else:
                            # 更新已售为可售
                            item.fields["flag_item"] = Field()
                            item["flag_item"] = 1
                            self.url_status_list_available.append([housing_url, '已售'])
                            yield item
                    else:
                        # 新增
                        yield scrapy.Request(url=housing_url, callback=self.housing_handle,
                                             meta={"item": copy.deepcopy(item)},
                                             headers={"User-Agent": random.choice(self.headers)}
                                             , dont_filter=True)
                page_dict_handle = response.xpath(
                    "//div[contains(@class, 'page-box')]/@page-data").extract_first()
                page_dict = json.loads(page_dict_handle)
                total_page = page_dict.get("totalPage")
                current_page = page_dict.get("curPage")
                if current_page < total_page:
                    next_page = ershoufang_plate_url + "pg" + str(current_page + 1) + "/"
                    yield scrapy.Request(url=next_page, callback=self.page_handle,
                                         meta={"ershoufang_plate_url": ershoufang_plate_url, 'region': region,
                                               "plate": plate}, headers={"User-Agent": random.choice(self.headers)},
                                         dont_filter=True)
        else:
            housing_num_flag = response.xpath("//span[@class='content__title--hl']/text()").extract_first()
            if housing_num_flag != '0':
                housing_list = response.xpath(
                    "//div[@class='content__list--item--main']/p[contains(@class,'content__list--item--title')]")
                for housing in housing_list:
                    # 列表页面字段获取
                    item = Item()
                    item.fields["城市"] = Field()
                    item["城市"] = self.city
                    item.fields["来源"] = Field()
                    item["来源"] = self.source
                    item.fields["租售状态"] = Field()
                    item["租售状态"] = 1
                    item.fields["区域"] = Field()
                    item["区域"] = region
                    item.fields["板块"] = Field()
                    item["板块"] = plate
                    housing_description = housing.xpath(
                        "./a/text()").extract_first().strip()
                    housing_url_handle = housing.xpath(
                        "./a/@href").extract_first()
                    item.fields["房源描述"] = Field()
                    item["房源描述"] = housing_description
                    housing_url = "https://sh.lianjia.com" + housing_url_handle
                    item.fields["房源链接"] = Field()
                    item["房源链接"] = housing_url
                    item.fields["房源状态"] = Field()
                    item["房源状态"] = "可租"
                    flag = [housing_url, '可租']
                    # 判断库内是否存在
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
                        yield scrapy.Request(url=housing_url, callback=self.housing_handle,
                                             meta={"item": copy.deepcopy(item)},
                                             headers={"User-Agent": random.choice(self.headers)}
                                             , dont_filter=True)

                total_page = int(response.xpath(
                    "//div[contains(@class, 'content__pg')]/@data-totalpage").extract_first())
                current_page = int(response.xpath(
                    "//div[contains(@class, 'content__pg')]/@data-curpage").extract_first())
                if current_page < total_page:
                    next_page = zufang_plate_url + "pg" + str(current_page + 1) + "/"
                    yield scrapy.Request(url=next_page, callback=self.page_handle,
                                         meta={"zufang_plate_url": zufang_plate_url, 'region': region,
                                               "plate": plate}, headers={"User-Agent": random.choice(self.headers)},
                                         dont_filter=True)

    def housing_handle(self, response):
        item = response.meta.get("item")
        租售状态 = item.get("租售状态")
        item1 = Item()
        # 二手房
        if not 租售状态:
            # 小区
            community = response.xpath(
                "//div[@class='communityName']/a[contains(@class,'info')]/text()").extract_first()
            # 地址
            address = response.xpath("//div[@class='areaName']").xpath("string(.)").extract_first().replace("所在区域",
                                                                                                            "").replace(
                "\xa0", '-')
            # 总价
            total_price = response.xpath("//div[contains(@class,'price')]/span[@class='total']/text()").extract_first()
            # 单价
            unit_price = response.xpath("//span[@class='unitPriceValue']/text()").extract_first()

            floor_info = response.xpath("//div[@class='room']/div[@class='subInfo']/text()").extract_first()
            # 总楼层
            total_floor = re.search("(共(.*?)层)", floor_info).group(1)
            # 所在楼层
            location_floor = re.search("(.*?)/", floor_info)
            if location_floor:
                location_floor = location_floor.group(1)
            item1.fields["总楼层"] = Field()
            item1["总楼层"] = total_floor
            item1.fields["所在楼层"] = Field()
            item1["所在楼层"] = location_floor
            item1.fields["小区"] = Field()
            item1["小区"] = community
            item1.fields["地址"] = Field()
            item1["地址"] = address
            item1.fields["总价"] = Field()
            item1["总价"] = total_price
            item1.fields["单价"] = Field()
            item1["单价"] = unit_price
            # 详细信息
            base_detail_info_list = response.xpath("//div[@class='base']/div[@class='content']/ul/li")
            for base_detail_info in base_detail_info_list:
                base_key_info = base_detail_info.xpath("./span/text()").extract_first()
                base_value_info = base_detail_info.xpath("./text()").extract_first()
                if base_key_info != "所在楼层":
                    item1.fields[base_key_info] = Field()
                    item1[base_key_info] = base_value_info

            # 交易详细信息
            trade_detail_info_list = response.xpath("//div[@class='transaction']/div[@class='content']/ul/li")
            for trade_detail_info in trade_detail_info_list:
                trade_info = trade_detail_info.xpath("./span/text()").extract()
                trade_key_info = trade_info[0]
                trade_value_info = trade_info[1]
                item1.fields[trade_key_info] = Field()
                if trade_key_info == "抵押信息":
                    trade_value_info = trade_value_info.strip()
                item1[trade_key_info] = trade_value_info

        # 租房
        else:
            # 单价
            unit_price = response.xpath("//p[@class='content__aside--title']/span/text()").extract_first()
            base_info = response.xpath("//p[@class='content__article__table']")
            # 租赁方式
            lease_way = base_info.xpath("./span[1]/text()").extract_first()
            # 房屋户型
            room_type = base_info.xpath("./span[2]/text()").extract_first()
            # 建筑面积
            room_area = base_info.xpath("./span[3]/text()").extract_first()
            # 房屋朝向
            room_orientation = base_info.xpath("./span[4]/text()").extract_first()
            item1.fields["单价"] = Field()
            item1["单价"] = unit_price
            item1.fields["租赁方式"] = Field()
            item1["租赁方式"] = lease_way
            item1.fields["房屋户型"] = Field()
            item1["房屋户型"] = room_type
            item1.fields["建筑面积"] = Field()
            item1["建筑面积"] = room_area
            item1.fields["房屋朝向"] = Field()
            item1["房屋朝向"] = room_orientation
            # 详细信息
            base_detail_info_list = response.xpath(
                "//div[@class='content__article__info']/ul/li[contains(text(),'：')]/text()")
            for base_detail_info in base_detail_info_list:
                base_detail_info_str = base_detail_info.extract()
                base_key_info = re.search("(.*?)：", base_detail_info_str).group(1)
                base_value_info = re.search(".*?：(.*)", base_detail_info_str).group(1)
                if base_key_info != "楼层":
                    item1.fields[base_key_info] = Field()
                    item1[base_key_info] = base_value_info
                else:
                    # 总楼层
                    location_floor = re.search("(.*?)/", base_value_info).group(1)
                    # 所在楼层
                    total_floor = re.search(".*?/(.*)", base_value_info).group(1)
                    item1.fields["总楼层"] = Field()
                    item1["总楼层"] = total_floor
                    item1.fields["所在楼层"] = Field()
                    item1["所在楼层"] = location_floor
        item1.update(item)
        yield item1
