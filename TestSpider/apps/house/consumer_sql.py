from TW.spider.Test_spider.apps.house.app_house import app
from celery.utils.log import get_task_logger
from TW.spider.Test_spider.apps.house.config import *
from TW.spider.Test_spider.pafunction.handle_sql import sh
from TW.spider.Test_spider.pafunction.handle_request import flag_item_toal


@app.task(
    name="sql",
    bind=True,
    rate_limit=RATE_LIMIT_SQL,
    autoretry_for=(),
    retry_kwargs={
        'max_retries': MAX_RETRIES_SQL,
        # 'countdown': COUNTDOWN_SQL
    },
    acks_late=True,
)
def sql(self, item):
    # 读取数据总量
    if TOTAL_ITEM == flag_item_toal:
        pass
    else:
        sh.do_sql(item)
