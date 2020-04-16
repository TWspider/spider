# -*- coding: utf-8 -*-
import datetime
import re

import requests
import scrapy
from lxml import etree
from .config import *


class AjkSpider(scrapy.Spider):
    name = 'ajk'
    allowed_domains = ['bannan.anjuke.com']
    start_urls = ['https://shanghai.anjuke.com/sale/?from=navigation']
    page_set = set()

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse,
                             headers=headers, dont_filter=True)

    def parse(self, response):
        print('parse')
        with open('1.html', 'w+', encoding='utf-8')as fp:
            fp.write(response.text)
        tree = etree.HTML(response.text)
        href_list = tree.xpath(
            '//div[@class="div-border items-list"]//div[@class="items"][1]/span[@class="elems-l"]/a/@href')
        area_list = tree.xpath(
            '//div[@class="div-border items-list"]//div[@class="items"][1]/span[@class="elems-l"]/a/text()')
        for index, href in enumerate(href_list):
            print(href, area_list[index])
            item = {}
            item['area'] = area_list[index]
            item['city'] = '上海'
            item['resource'] = '安居客'
            print(href, area_list[index])
            yield scrapy.Request(url=href, headers=headers, callback=self.second_parse, dont_filter=True,
                                 meta={'item': item})
            # break

    def second_parse(self, response):
        print('second_parse')
        item = response.meta['item']
        with open('2.html', 'w+', encoding='utf-8')as fp:
            fp.write(response.text)
        tree = etree.HTML(response.text)
        href_list = tree.xpath('//div[@class="house-title"]/a/@href')
        print(len(href_list), '======================================一页有多少条数据')
        for href in href_list:
            yield scrapy.Request(url=href, headers=headers, callback=self.third_parse, dont_filter=True,
                                 meta={'item': item})
            # break
        try:
            next_url = tree.xpath('//div[@id="content"]/div[4]/div[7]/a[last()]/@href')[0]
            if next_url not in self.page_set:
                self.page_set.add(next_url)
                print(next_url)
                yield scrapy.Request(url=next_url, headers=headers, callback=self.second_parse, dont_filter=True,
                                     meta={'item': item})
            else:
                print('已经到末页', next_url)
        except:
            print('获取下一页有误')

    def third_parse(self, response):
        print('third_parse', response.url)
        with open('3.html', 'w+', encoding='utf-8')as fp:
            fp.write(response.text)
        item = response.meta['item']
        tree = etree.HTML(response.text)
        # try:
        item['house_desc'] = tree.xpath('//*[@class="clearfix title-guarantee"]//h3/text()')[0].strip()
        item['total_price'] = tree.xpath('//span[@class="light info-tag"]/em/text()')[0].strip()
        item['door_model'] = tree.xpath('//*[@id="content"]//ul/li[2]/div[2]/text()')[0].strip().replace("\n",
                                                                                                         '').replace(
            "\t", "")
        item['build_size'] = tree.xpath('//*[@id="content"]//ul/li[5]/div[2]/text()')[0].strip()
        try:
            item['Pub_company'] = tree.xpath('//*[@id="broker-wrap2"]//p[1]/a/text()')[0].strip()
        except:
            item['Pub_company'] = 0
        item['agent'] = tree.xpath('//*[@id="broker-wrap2"]/div[1]/div/div/text()')[0].strip()
        item['community'] = tree.xpath('//*[@id="content"]//ul/li[1]/div[2]/a/text()')[0].strip()
        item['addr'] = tree.xpath('//*[@id="content"]//ul/li[4]/div[2]/p')[0].xpath('string(.)').strip().replace("\n",
                                                                                                                 "").replace(
            " ", "")
        item['build_time'] = tree.xpath('//*[@id="content"]//ul/li[7]/div[2]/text()')[0].strip()
        item['property_year'] = tree.xpath('//*[@id="content"]//ul/li[13]/div[2]/text()')[0].strip()
        item['direction'] = tree.xpath('//*[@id="content"]//ul/li[8]/div[2]/text()')[0].strip()
        floor_desc = tree.xpath('//*[@id="content"]//ul/li[11]/div[2]/text()')[0].strip()
        item['total_floor'] = re.findall('共(\d+)层', floor_desc)[0]
        try:
            item['floor'] = re.findall('(\w+)[\(\（]', floor_desc)[0]
        except:
            item['floor'] = 0
        item['elevator'] = tree.xpath('//*[@id="content"]//ul/li[14]/div[2]/text()')[0].strip()
        item['per_price'] = tree.xpath('//*[@id="content"]//ul/li[3]/div[2]/text()')[0].strip()
        item['decoration'] = tree.xpath('//*[@id="content"]//ul/li[12]/div[2]/text()')[0].strip()
        item['house_years'] = tree.xpath('//*[@id="content"]//ul/li[15]/div[2]/text()')[0].strip()
        # item['house_use'] = tree.xpath('//*[@id="content"]//ul/li[16]/div[2]/text()')[0].strip()
        item['house_url'] = response.url.split('/jump?')[0]
        item['insert_time'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        item['rental_status'] = 0
        item['resource_status'] = '可售'
        yield item
        # except Exception as e:
        #     print('有误', e)
