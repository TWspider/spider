from scrapy.selector import Selector
from Chihiro.pafunction.handle_item import ItemHandle
from Chihiro.apps.house.app_house import app
from celery.utils.log import get_task_logger
from Chihiro.apps.house.config import *


logger = get_task_logger(__name__)


@app.task(
    name="item_1",
    bind=True,
    rate_limit=RATE_LIMIT_ITEM,
    autoretry_for=(),
    retry_kwargs={
        'max_retries': MAX_RETRIES_ITEM,
        # 'countdown': COUNTDOWN
    },
    acks_late=True,
)
def item(self, response):
    pass


@app.task(
    name="item_2",
    bind=True,
    rate_limit=RATE_LIMIT_ITEM,
    autoretry_for=(),
    retry_kwargs={
        'max_retries': MAX_RETRIES_ITEM,
        # 'countdown': COUNTDOWN
    },
    acks_late=True,
)
def item(self, response):
    pass


@app.task(
    name="item_2",
    bind=True,
    rate_limit=RATE_LIMIT_ITEM,
    autoretry_for=(),
    retry_kwargs={
        'max_retries': MAX_RETRIES_ITEM,
        # 'countdown': COUNTDOWN
    },
    acks_late=True,
)
def item(self, response):
    pass