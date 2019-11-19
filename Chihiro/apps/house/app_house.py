from celery import Celery

app = Celery(
    'app_house',
    include=[
        'consumer_request',
        'consumer_item',
        'consumer_sql',
    ]
)

app.config_from_object("config")

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)
