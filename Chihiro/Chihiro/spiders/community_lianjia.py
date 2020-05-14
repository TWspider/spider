# -*- coding: utf-8 -*-
import scrapy
import re
import json
from copy import deepcopy
from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import random
import logging
from scrapy.utils.project import get_project_settings
from scrapy import Item, Field


class Chihiro(scrapy.Spider):
    name = 'community_lianjia'
    base_url = "https://sh.lianjia.com"
    start_urls = ['https://sh.lianjia.com/xiaoqu/']
    PropertyCity = '上海'
    Resource = '链家'
    custom_settings = {
        # 基本参数
        "BOT_NAME": name,
        "PropertyCity": PropertyCity,
        "Resource": Resource,
        # 请求参数
        "DEFAULT_REQUEST_HEADERS": {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'gzip',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        },
        # "DOWNLOAD_DELAY": 0.3,
        # "CONCURRENT_REQUESTS": 1,
        "RETRY_HTTP_CODES": [302, 403, 502],
        "RETRY_TIMES": 3,
        "DOWNLOADER_MIDDLEWARES": {
            # 'Chihiro.middleware_request.ChihiroDownloaderMiddleware': 543,
            'Chihiro.middleware_request.UserAgent_Middleware': 543,
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
        # "LOG_FILE": "Chihiro.txt"
    }


    def parse(self, response):
        '''
        //div[@class='m-filter']/div[@class='position']/dl[2]/dd/div/div/a
        :param response:
        :return:
        '''
        region_xpath_list = response.xpath(
            "//div[@class='m-filter']/div[@class='position']/dl[2]/dd/div/div/a")
        for region_xpath in region_xpath_list:
            region = region_xpath.xpath("./text()").extract_first()
            region_url_handle = region_xpath.xpath("./@href").extract_first()
            region_url = self.base_url + region_url_handle
            yield scrapy.Request(url=region_url, callback=self.plate_handle, meta={"region": region})

    def plate_handle(self, response):
        region = response.meta.get("region")
        plate_xpath_list = response.xpath(
            "//div[@class='m-filter']/div[@class='position']/dl[2]/dd/div/div[2]/a")
        for plate_xpath in plate_xpath_list:
            plate = plate_xpath.xpath("./text()").extract_first()
            plate_url_handle = plate_xpath.xpath("./@href").extract_first()
            plate_url = self.base_url + plate_url_handle
            yield scrapy.Request(url=plate_url, callback=self.page_handle,
                                 meta={"region": region, "plate": plate, "plate_url": plate_url},dont_filter=True)

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
                "//div[@class='content']/div[@class='leftContent']/ul[@class='listContent']/li")
            for housing in housing_list:
                # 列表页面字段获取
                item = Item()
                item.fields["AreaName"] = Field()
                item["AreaName"] = region
                item.fields["PlateName"] = Field()
                item["PlateName"] = plate
                CommunityUrl = housing.xpath("./div[@class='info']/div[@class='title']/a/@href").extract_first()
                PropertyCommunity = housing.xpath("./div[@class='info']/div[@class='title']/a/text()").extract_first()
                item.fields["PropertyCommunity"] = Field()
                item["PropertyCommunity"] = PropertyCommunity
                item.fields["CommunityUrl"] = Field()
                item["CommunityUrl"] = CommunityUrl
                yield scrapy.Request(url=CommunityUrl, callback=self.housing_handle,
                                     meta={"item": deepcopy(item)})
            page_dict_handle = response.xpath(
                "//div[contains(@class, 'page-box')]/@page-data").extract_first()
            page_dict = json.loads(page_dict_handle)
            total_page = page_dict.get("totalPage")
            current_page = page_dict.get("curPage")
            if current_page < total_page:
                next_page = plate_url + "pg" + str(current_page + 1) + "/"
                yield scrapy.Request(url=next_page, callback=self.page_handle,
                                     meta={"plate_url": plate_url, 'region': region,
                                           "plate": plate})

    def housing_handle(self, response):
        item = response.meta.get("item")
        item1 = Item()
        PropertyAddress = response.xpath(
            "//div[@class='detailDesc']/text()").extract_first()
        PriceUnit = response.xpath(
            "//span[@class='xiaoquUnitPrice']/text()").extract_first()
        detail_community = response.xpath("//div[@class='xiaoquInfo']")
        BuildedTime = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][1]/span[@class='xiaoquInfoContent']/text()").extract_first()
        BuildingType = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][2]/span[@class='xiaoquInfoContent']/text()").extract_first()
        PropertyFee = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][3]/span[@class='xiaoquInfoContent']/text()").extract_first()
        PropertyCompany = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][4]/span[@class='xiaoquInfoContent']/text()").extract_first()
        Developers = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][5]/span[@class='xiaoquInfoContent']/text()").extract_first()
        TotalBuilding = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][6]/span[@class='xiaoquInfoContent']/text()").extract_first()
        TotalHouseholds = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][7]/span[@class='xiaoquInfoContent']/text()").extract_first()
        NearbyStores = detail_community.xpath(
            "./div[@class='xiaoquInfoItem'][8]/span[@class='xiaoquInfoContent']").xpath("string(.)").extract_first()
        item1.fields["PropertyAddress"] = Field()
        item1["PropertyAddress"] = PropertyAddress
        item1.fields["PriceUnit"] = Field()
        item1["PriceUnit"] = PriceUnit

        item1.fields["BuildedTime"] = Field()
        item1["BuildedTime"] = BuildedTime
        item1.fields["BuildingType"] = Field()
        item1["BuildingType"] = BuildingType
        item1.fields["PropertyFee"] = Field()
        item1["PropertyFee"] = PropertyFee
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
        item1.update(item)
        yield item1
