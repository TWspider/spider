from Chihiro.apps.house.app_house import app
from celery.utils.log import get_task_logger
import requests
from Chihiro.pafunction.handle_request import RequestHandle
from scrapy.selector import Selector
from Chihiro.apps.house.config import *

logger = get_task_logger(__name__)


# 初始页要设置字典,剩余的页数要增加kwargs参数,以及kwargs更新

@app.task(
    name="page_1",
    bind=True,
    rate_limit=RATE_LIMIT_REQUEST,
    autoretry_for=(
            requests.Timeout, requests.ConnectionError, requests.HTTPError, requests.exceptions.ChunkedEncodingError),
    retry_kwargs={
        'max_retries': MAX_RETRIES_REQUEST,
        # 'countdown': COUNTDOWN
    },
    acks_late=True,
)
def page(self, url):
    DEFAULT_REQUEST_HEADERS.update(RequestHandle().get_header())
    res = requests.get(url=url, headers=DEFAULT_REQUEST_HEADERS)
    res_text = res.text
    res_status = res.status_code
    if res_status != 200:
        raise requests.HTTPError
    else:
        response = Selector(response=res)
        url_list = response.xpath("//ul[contains(@class,'tap_show')]/li/a")
        for url in url_list:
            url = url.xpath("./@href").extract_first()
            region_url = BASE_URL + url
            app.send_task("page_2", args=(region_url, {"res_1": res_text}), queue=PRODUCTER_QUEUE_REQUEST)


@app.task(
    name="page_2",
    bind=True,
    rate_limit=RATE_LIMIT_REQUEST,
    autoretry_for=(requests.Timeout, requests.ConnectionError, requests.HTTPError),
    retry_kwargs={
        'max_retries': MAX_RETRIES_REQUEST,
        # 'countdown': COUNTDOWN_REQUEST
    },
    acks_late=True,
)
def page(self, url, kwargs):
    DEFAULT_REQUEST_HEADERS.update(RequestHandle().get_header())
    res = requests.get(url=url, headers=DEFAULT_REQUEST_HEADERS)
    res_text = res.text
    res_status = res.status_code
    if res_status != 200:
        raise requests.HTTPError
    else:
        response = Selector(response=res)
        kwargs.update({"res_2": res_text})
        url_list = response.xpath(
            "//div[@class='tagbox_wrapper_main']/div[@class='tagbox_wrapper_cd']/ul[contains(@class,'tap_show')]/li[position()>1]/a")
        for url in url_list:
            url = url.xpath("./@href").extract_first()
            plate_url = BASE_URL + url
            app.send_task("page_3", args=(plate_url, kwargs), queue=PRODUCTER_QUEUE_REQUEST)


@app.task(
    name="page_3",
    bind=True,
    rate_limit=RATE_LIMIT_REQUEST,
    autoretry_for=(requests.Timeout, requests.ConnectionError, requests.HTTPError),
    retry_kwargs={
        'max_retries': MAX_RETRIES_REQUEST,
        # 'countdown': COUNTDOWN
    },
    acks_late=True,
)
def page(self, url, kwargs):
    DEFAULT_REQUEST_HEADERS.update(RequestHandle().get_header())
    res = requests.get(url=url, headers=DEFAULT_REQUEST_HEADERS)
    res_text = res.text
    res_status = res.status_code
    if res_status != 200:
        raise requests.HTTPError
    else:
        response = Selector(response=res)
        kwargs.update({"res_3": res_text})
        total_num = response.xpath(
            "//div[@class='hs-counts']/h3/span/text()").extract_first()
        if total_num:
            total_num = int(total_num)
            reminder_num = total_num % 20
            page_num = total_num // 20
            if total_num:
                if reminder_num:
                    for i in range(1, page_num + 2):
                        housing_list_url = url + "g" + str(i) + "/"
                        app.send_task("page_4", args=(housing_list_url, kwargs), queue=PRODUCTER_QUEUE_REQUEST)
                else:
                    for i in range(1, page_num + 1):
                        housing_list_url = url + "g" + str(i) + "/"
                        app.send_task("page_4", args=(housing_list_url, kwargs), queue=PRODUCTER_QUEUE_REQUEST)


@app.task(
    name="page_4",
    bind=True,
    rate_limit=RATE_LIMIT_REQUEST,
    autoretry_for=(requests.Timeout, requests.ConnectionError, requests.HTTPError),
    retry_kwargs={
        'max_retries': MAX_RETRIES_REQUEST,
        # 'countdown': COUNTDOWN
    },
    acks_late=True,
)
def page(self, url, kwargs):
    DEFAULT_REQUEST_HEADERS.update(RequestHandle().get_header())
    res = requests.get(url=url, headers=DEFAULT_REQUEST_HEADERS)
    res_text = res.text
    res_status = res.status_code
    if res_status != 200:
        raise requests.HTTPError
    else:
        kwargs.update({"res_4": res_text})
        app.send_task("item_1", args=(kwargs,), queue=PRODUCTER_QUEUE_REQUEST_NEXT)
