from datetime import timedelta
from kombu import Queue, Exchange, binding

BROKER_URL = 'amqp://guest:guest@localhost:5672//'

# CELERY_RESULT_BACKEND = "amqp://admin:qwer@localhost:5672//"

CELERY_TIMEZONE = "Asia/Shanghai"
BROKER_HEARTBEAT = None

# test_celery.task.add这个任务进去test队列并routeing_key为test.add
# 默认生成一个test交换机
# CELERY_ROUTES = {
#     'test_celery.task.add': {
#         'queue': 'test',
#         'routing_key': 'test.add',
#     }
# }

# CELERYBEAT_SCHEDULE = {
#     "add": {
#         "task": "task.add",
#         "schedule": timedelta(seconds=2),
#         "args": (16, 16)
#     }
# }
#
# test = Exchange('test', type='direct')
#
# CELERY_QUEUES = (
#     Queue(
#         'test',
#         [binding(exchange=test, routing_key='test'), ],
#         queue_arguments={'x-queue-mode': 'lazy', 'x-max-priority': 10},
#         max_priority=10
#     ),
#     Queue(
#         'test1',
#         [binding(exchange=test, routing_key='test1'), ],
#         queue_arguments={'x-queue-mode': 'lazy', 'x-max-priority': 10},
#         max_priority=10
#     ),
# )
