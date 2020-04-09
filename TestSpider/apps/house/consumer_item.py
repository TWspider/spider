from scrapy.selector import Selector
from TW.spider.Test_spider.pafunction.handle_item import ItemHandle
from TW.spider.Test_spider.apps.house.app_house import app
from celery.utils.log import get_task_logger
from TW.spider.Test_spider.apps.house.config import *

logger = get_task_logger(__name__)


@app.task(
    name="item_1",
    bind=True,
    rate_limit=RATE_LIMIT_ITEM,
    acks_late=True,
)
def item(self, response):
    pass


@app.task(
    name="item_2",
    bind=True,
    rate_limit=RATE_LIMIT_ITEM,
    acks_late=True,
)
def item(self, response):
    pass


@app.task(
    name="item_3",
    bind=True,
    rate_limit=RATE_LIMIT_ITEM,
    acks_late=True,
)
def item(self, response):
    pass
