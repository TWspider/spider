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
from scrapy.selector import Selector
from urllib import parse


class Chihiro(scrapy.Spider):
    name = 'news_sogou'
    base_url = "https://weixin.sogou.com"
    kw_list = [
            '太平洋房屋',
            '太平洋房产',
            '太平洋中介',
            '太屋网',
            '太屋集团',
            '菁英地产',
        ]

    # PropertyCity = '上海'
    # Resource = '安居客'
    # RentalStatus = 1
    # HouseStatus = '可租'
    custom_settings = {
        # 基本参数
        "BOT_NAME": name,
        # "PropertyCity": PropertyCity,
        # "Resource": Resource,
        # "RentalStatus": RentalStatus,
        # "HouseStatus": HouseStatus,
        # 请求参数
        "DEFAULT_REQUEST_HEADERS": {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'weixin.sogou.com',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36',
        },
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        "RETRY_HTTP_CODES": [400, 500],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            'Chihiro.middleware_request.NewsUserAgentMiddleware': 543,
        },
        # 清洗参数
        "SPIDER_MIDDLEWARES": {
            # 'Chihiro.middleware_item.ChihiroSpiderMiddleware': 500,
        },
        # 业务参数
        "ITEM_PIPELINES": {
            # 'Chihiro.middleware_sql.ChihiroPipeline': 300,
        },
        # 错误记录
        # ERROR_RECORD = True
        # 日志
        # "LOG_LEVEL": 'INFO',
        # "LOG_FILE": "Chihiro.txt"
    }

    def get_real_url_handle(self, url):
        b = int(random.random() * 100) + 1
        a = url.find("url=")
        url = url + "&k=" + str(b) + "&h=" + url[a + 4 + 21 + b: a + 4 + 21 + b + 1]
        return url

    def start_requests(self):
        for kw in self.kw_list:
            start_page = 1
            for page in range(start_page, 11):
                item = Item()
                item.fields["SearchWord"] = Field()
                item.fields["Page"] = Field()
                item["SearchWord"] = kw
                item["Page"] = page
                start_url = 'https://weixin.sogou.com/weixin?query={}&type=2&page={}&ie=utf8'.format(
                    parse.quote(kw), str(page))
                yield scrapy.Request(url=start_url, callback=self.parse,
                                     meta={"start_url": start_url, "item": item})

    def parse(self, response):
        req = response.meta.get("req")
        item = response.meta.get("item")
        url_list_handle = Selector(text=response.text)

        url_list = url_list_handle.xpath("//div[@class='txt-box']/h3/a/@href").extract()
        for index, url in enumerate(url_list):
            item1 = Item()
            item1.fields["Located"] = Field()
            item1["Located"] = index
            url = self.base_url + url
            url = self.get_real_url_handle(url)
            item1.update(item)
            yield scrapy.Request(url=url, callback=self.handle_1, meta={"req": req, "item1": item1})

    def handle_1(self, response):
        item1 = response.meta.get("item1")
        req = response.meta.get("req")
        fragments = re.findall("url \+= '(.*?)'", response.text, re.S)
        detail_url = ''
        for j in fragments:
            detail_url += j
        item2 = Item()
        item2.fields["NewUrl"] = Field()
        item2["NewUrl"] = detail_url
        item2.update(item1)
        yield scrapy.Request(url=detail_url, callback=self.parse, meta={"req": req, "item2": item2, "last_page": True})

    def handle_2(self, response):
        item2 = response.meta.get("item2")
        item3 = Item()
        item3.fields["Title"] = Field()
        item3.fields["Content"] = Field()
        res_text = response.text
        res3_handle = Selector(text=res_text)
        # title
        title = res3_handle.xpath('//meta[@property="og:title"]/@content').extract_first()
        try:
            content = res3_handle.xpath("//div[@id='js_content']").xpath("string(.)").extract_first()
            content = content.strip()
        except Exception as e:
            content = None
        item3["Title"] = title
        item3["Content"] = content
        item3.update(item2)
        yield item3
