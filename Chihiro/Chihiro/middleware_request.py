# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals


class ChihiroDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class UserAgent_Middleware():
    def process_request(self, request, spider):
        ua = UserAgent()
        request.headers['User-Agent'] = ua.random


import base64


class IpAgent_Middleware():
    def process_request(self, request, spider):
        # ua = UserAgent()
        # request.headers['User-Agent'] = ua.random
        request.meta['proxy'] = "https://t18449818935473:jg4cg2j9@tps161.kdlapi.com:15818/"
        # request.headers["Proxy-Authorization"] = proxyAuth


import requests
import time
import random
from scrapy.http import HtmlResponse


class RequestsMiddleware():
    def process_request(self, request, spider):
        headers = spider.custom_settings.get("DEFAULT_REQUEST_HEADERS")
        try:
            response = requests.get(url=request.url, headers=headers)
            body = response.content
            return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                status=200)
        except:
            return HtmlResponse(url=request.url, body='', request=request, encoding='utf-8',
                                status=400)


class NewsUserAgentMiddleware():
    def process_request(self, request, spider):
        last_page = request.meta.get("last_page")
        headers = spider.custom_settings.get("DEFAULT_REQUEST_HEADERS")
        if last_page:
            req = request.meta.get("req")
            response = req.get(url=request.url, headers=headers)
            body = response.content
            flag = body.decode("utf8")
            if "异常访问" in flag:
                print("ip被封，正在重试。。。")
                time.sleep(random.uniform(5, 6))
                # 更换ip
                pass
                return HtmlResponse(url=request.url, body='', request=request, encoding='utf-8',
                                    status=400)
            else:
                return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                    status=200)
        else:
            start_url = request.meta.get("start_url")
            headers.update(
                {
                    "Referer": start_url
                }
            )
            if start_url:
                req = requests.session()
                request.meta["req"] = req
            else:
                req = request.meta.get("req")
            response = req.get(url=request.url, headers=headers)
            body = response.content
            flag = body.decode("utf8")
            if "异常访问" in flag:
                print("ip被封，正在重试。。。")
                time.sleep(random.uniform(2, 3))
                # 更换ip
                pass
                return HtmlResponse(url=request.url, body='', request=request, encoding='utf-8',
                                    status=400)
            else:
                return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                    status=200)


from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import os
from fake_useragent import UserAgent
import platform


class ChromeDownloaderMiddleware(object):
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # 设置无界面
        self.current_path = os.path.abspath("")
        os_name = platform.system()
        if "Windows" in os_name:
            self.driver = webdriver.Chrome(chrome_options=options,
                                           executable_path="{}\chromedriver.exe".format(
                                               self.current_path))  # 初始化Chrome驱动
        else:
            self.driver = webdriver.Chrome(chrome_options=options,
                                           executable_path="{}/chromedriver_linux".format(
                                               self.current_path))  # 初始化Chrome驱动

    def __del__(self):
        self.driver.close()

    def process_request(self, request, spider):
        try:
            print('Chrome driver begin...')
            self.driver.get(request.url)  # 获取网页链接内容
            return HtmlResponse(url=request.url, body=self.driver.page_source, request=request, encoding='utf-8',
                                status=200)  # 返回HTML数据
        except TimeoutException:
            return HtmlResponse(url=request.url, request=request, encoding='utf-8', status=500)
        finally:
            print('Chrome driver end...')


from twisted.internet.defer import Deferred
from scrapy.http import HtmlResponse
import asyncio
from pyppeteer import launch


def as_deferred(f):
    """Transform a Twisted Deffered to an Asyncio Future"""

    return Deferred.fromFuture(asyncio.ensure_future(f))


class PuppeteerMiddleware:
    async def _process_request(self, request, spider):
        """Handle the request using Puppeteer"""
        browser = await launch(headless=False)
        page = await browser.newPage()
        try:
            print(request.url)
            await page.goto(request.url)
            page_source = await page.content()
            print(page_source)
            return HtmlResponse(url=request.url, body=page_source, request=request, encoding='utf-8',
                                status=200)  # 返回HTML数据
        except TimeoutException:
            return HtmlResponse(url=request.url, request=request, encoding='utf-8', status=500)
        finally:
            print('Chrome driver end...')

    def process_request(self, request, spider):
        """Check if the Request should be handled by Puppeteer"""
        # if request.meta.get('render') == 'pyppeteer':

        return as_deferred(self._process_request(request, spider))

        # return None
