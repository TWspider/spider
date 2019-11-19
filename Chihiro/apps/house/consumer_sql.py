from Chihiro.apps.house.app_house import app
from celery.utils.log import get_task_logger
from Chihiro.apps.house.config import *


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
    pass
