# -*- coding: utf-8 -*-
from io import BytesIO
from html.parser import HTMLParser
from fontTools.ttLib.ttFont import TTFont
import base64
import logging
from copy import deepcopy
import scrapy
import re
import json
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import pandas as pd
from sqlalchemy import create_engine
import datetime

class Chihiro(scrapy.Spider):
    name = 'anjuke_zf'
    start_urls = ['https://m.anjuke.com/sh/rent/all/a0_0-b0-0-0-f0/']
    PropertyCity = '上海'
    Resource = '安居客'
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
            # 'cookie': 'wmda_new_uuid=1; wmda_uuid=65ba8454b17fceab3bf3a71e2cf7d588; ctid=11; ajkAuthTicket=TT=209b5da00e384bca2e43ca90ba6466b0&TS=1577428649321&PBODY=jC6iZYbdfcKNikHSdXjJNePUh0uHE2mZLDX04l6kmuXDL6rtG9uPeuizkTebekQ5P7tg7YlYjLYXLiXJeNbWsmJV4tJ78F9sd9Ii3EYY8Uh5oJJnUj1nhuOOeTZljaNvmJ-Fqc4n-leC-n25A7T7xmEfwmnmZd2drAT1ApTpzwE&VER=2; wmda_visited_projects=%3B6289197098934%3B6145577459763; app_cv=unknown; aQQ_ajkguid=DD8C0A80-7B41-62ED-1DF8-13E233042267; lps="/sh/sale/?from=anjuke_home|"; sessid=A5FA696C-6698-8703-334D-2B7958DD0A6E; _ga=GA1.2.1779264905.1578018563; twe=2; 58tj_uuid=01c4d193-8949-46fc-88c2-41088bd3175e; als=0; new_uv=3; _gid=GA1.2.1632023350.1578293307; ctid=11; __xsptplus8=8.5.1578293307.1578293899.6%232%7Cwww.baidu.com%7C%7C%7C%25E5%25AE%2589%25E5%25B1%2585%25E5%25AE%25A2%2B%25E4%25BA%258C%25E6%2589%258B%25E6%2588%25BF%25E6%258E%25A5%25E5%258F%25A3%7C%23%236-bInTZ7Yi4i7hioacL1TrOqJAlNn1_n%23; ajk_view_visit={%22timeStamp%22:%222020/1/6%22%2C%22rent_view%22:6}; xzfzqtoken=DuT47ssWj0ZV99UBpuxMYPB%2F%2FMgIDPoLxpVf%2FVb7WU2EQGCVOCwhuvg56XbyXNmXin35brBb%2F%2FeSODvMgkQULA%3D%3D',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        },
        "DOWNLOAD_DELAY": 0.2,
        "CONCURRENT_REQUESTS": 5,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            'Chihiro.middleware_request.IpAgent_Middleware': 543,
            'Chihiro.middleware_request.UserAgent_Middleware': 500,
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
        "LOG_LEVEL": 'INFO',
        "LOG_FILE": "anjuke_zf.txt"
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

    def font_desecret(self, font_url, string_list):
        bin_data = base64.decodebytes(font_url.encode())
        font = TTFont(BytesIO(bin_data))
        font.saveXML("text.xml")
        font = TTFont(BytesIO(bin_data))
        c = font['cmap'].tables[0].ttFont.tables['cmap'].tables[0].cmap
        res_ls = []
        for string in string_list:
            res_str = ''
            for char in string:
                try:
                    decode_num = ord(char)
                    num = c[decode_num]
                    num = int(num[-2:]) - 1
                except:
                    num = "."
                res_str += str(num)
            res_ls.append(res_str)
        return res_ls

    def parse(self, response):
        area_list = response.xpath("//div[@id='regioninfo']/ul[@class='regionlist']/li[position()>1][position()<17]")
        for area in area_list:
            area_num = area.xpath("./@data-type").extract_first()
            # 区域
            area_name = area.xpath("./span/text()").extract_first()
            plate_list = response.xpath(
                "//div[@id='{}']/div[@class='blocklist']/a[position()>1]".format(area_num))
            for plate in plate_list:
                i = ItemLoader(item=ChihiroItem(), selector=plate)
                # 板块名
                i.add_value("AreaName", area_name)
                i.add_xpath("PlateName", "./text()")
                item = i.load_item()
                url = plate.xpath("./@data-href").extract_first()
                yield scrapy.Request(url=url, callback=self.handle_1, meta={"item": deepcopy(item)})

    def handle_1(self, response):
        res_text = response.text
        item = response.meta.get("item")
        font_url = re.search("base64,(.*?)'\)", res_text).group(1)
        base_url = "https://m.anjuke.com/zufang/m/house/api_houselist_data_Jgs?page=1&search_firstpage=1&search_param={%s}&font_encrypt=%s"
        base_search_param = re.search('search_param: {(.*?)}', res_text).group(1).replace('"page_size":30',
                                                                                          '"page_size":100')
        font_encrypt = re.search('font_encrypt: "(.*?)"', res_text).group(1)
        for i in range(1, 6):
            search_param = base_search_param + ',"room_nums":{}'.format(str(i))
            url = base_url % (search_param, font_encrypt)
            yield scrapy.Request(url=url, callback=self.handle_2,
                                 meta={"font_url": font_url, "search_param": search_param,
                                       "font_encrypt": font_encrypt, "item": deepcopy(item)})

    def handle_2(self, response):
        item = response.meta.get("item")
        font_url = response.meta.get("font_url")
        search_param = response.meta.get("search_param")
        font_encrypt = response.meta.get("font_encrypt")
        base_url = "https://m.anjuke.com/zufang/m/house/api_houselist_data_Jgs?page=%s&search_firstpage=1&search_param={%s}&font_encrypt=%s"
        res_json = json.loads(response.text)
        total = res_json.get("data").get("total")
        total_page = int(total) // 100 + 2
        for i in range(1, total_page):
            url = base_url % (str(i), search_param, font_encrypt)
            yield scrapy.Request(url=url, callback=self.handle_3, meta={"font_url": font_url, "item": deepcopy(item)})

    def handle_3(self, response):
        item = response.meta.get("item")
        res_json = json.loads(response.text)
        font_url = response.meta.get("font_url")
        data_list = res_json.get("data").get("list")
        for data in data_list:
            i = ItemLoader(item=ChihiroItem())
            # 链接
            string_list = []
            house_url = data.get("front_message").get("detail_url")
            i.add_value("HouseUrl", house_url)
            house_base1 = data.get("property").get("base")
            # 描述
            house_desc = house_base1.get("title")
            i.add_value("HouseDesc", house_desc)
            house_base2 = house_base1.get("attribute")
            room_num = house_base2.get("room_num")
            hall_num = house_base2.get("hall_num")
            toilet_num = house_base2.get("toilet_num")
            area_num = house_base2.get("area_num")
            price = house_base2.get("price")
            html_parser = HTMLParser()
            res_price = html_parser.unescape(price)
            res_room_num = html_parser.unescape(room_num)
            res_hall_num = html_parser.unescape(hall_num)
            res_area_num = html_parser.unescape(area_num)
            string_list.append(res_room_num)
            string_list.append(res_hall_num)
            string_list.append(res_area_num)
            string_list.append(res_price)
            res = self.font_desecret(font_url=font_url, string_list=string_list)
            # 房型
            room_type = res[0] + "室" + res[1] + "厅" + toilet_num + "卫"
            i.add_value("HouseType", room_type)
            # 面积
            room_area = res[2]
            i.add_value("BuildingSquare", room_area)

            # 单价
            price = res[3]
            i.add_value("PriceUnit", price)

            house_base3 = data.get("community").get("base")
            # 小区
            community = house_base3.get("name")
            i.add_value("PropertyCommunity", community)

            # 地址
            address = house_base3.get("address")
            i.add_value("PropertyAddress", address)

            house_base4 = data.get("property").get("base").get("attribute")
            # 总楼层
            total_floor = house_base4.get("total_floor")
            i.add_value("TotalFloor", total_floor)
            # 装修
            fitment_name = house_base4.get("fitment_name")
            i.add_value("FixTypeName", fitment_name)

            # 朝向
            orient = house_base4.get("orient")
            i.add_value("HouseDirection", orient)
            item1 = i.load_item()
            item1.update(item)
            yield scrapy.Request(url=house_url, callback=self.handle_4, meta={"item1": deepcopy(item1)})

    def handle_4(self, response):
        item1 = response.meta.get("item1")
        i = ItemLoader(item=ChihiroItem(), response=response)
        # PubCompany        发布公司    pass
        i.add_xpath("PubCompany", "//div[contains(@class,'prop-broker-text')]/text()")
        # Agent             经纪人    pass
        i.add_xpath("Agent", "//div[contains(@class,'prop-broker-name')]/text()")
        # Floor
        floor = response.xpath("//label[contains(text(),'楼层')]/..").extract_first()
        try:
            floor = re.search(" (.*?)层\(", floor).group(1).lstrip() + "层"
        except:
            floor = None
        i.add_value("Floor", floor)
        item2 = i.load_item()
        item2.update(item1)
        if self.is_finished():
            pipeline = self.crawler.spider.pipeline
            scaned_url_list = pipeline.scaned_url_list
            url_list = pipeline.url_list
            housing_trade_list = [x for x in url_list if x not in scaned_url_list]
            logging.info(housing_trade_list)
            for housing_url in housing_trade_list:
                yield scrapy.Request(url=housing_url, callback=self.house_status_handle)
        yield item2

    def is_finished(self):
        flag_queue = len(self.crawler.engine.slot.scheduler)
        if flag_queue:
            return False
        return True

    def house_status_handle(self, response):
        # 验证码
        url = response.url
        item = {}
        flag = response.xpath("//h1[@class='prop-title']/b/text()").extract_first()
        if flag:
            pass
        else:
            item["flag_remaining"] = True
            item["HouseUrl"] = url
            yield item
