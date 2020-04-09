from celery import Celery

app = Celery('testapp', include=[
    'task',
])

app.config_from_object("config")
# apps.conf.task_default_queue = 'test'
# apps.conf.task_default_exchange = 'test'
# apps.conf.task_default_exchange_type = 'fanout'

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)
# if __name__ == '__main__':
#     apps.start()
