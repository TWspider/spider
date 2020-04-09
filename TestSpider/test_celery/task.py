from TW.spider.Test_spider.test_celery.testapp import app
from celery.utils.log import get_task_logger
import requests
import re
import datetime
from scrapy.selector import Selector

logger = get_task_logger(__name__)


@app.task(
    name="test",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 5,
        # 'countdown': 1
    },
    acks_late=True,
)
def test(self, start_url):
    print(start_url.text)

