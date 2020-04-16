from TW.spider.Test_spider.test_celery.testapp import app
import requests

res = requests.get(url="https://sh.lianjia.com/ershoufang/")
START_TASK = "test"
START_URL = (res,)
QUEUE = "test"

app.send_task(START_TASK, args=START_URL, queue=QUEUE)

# 异步
# t = add.apply_async(args=(1, 2))
# res = t.get(propagate=False)
# print(res)

# for _ in range(1, 1000000):
#     a = upper.apply_async(args=("A",))
#     # 打印异常，不报错get(propagate=False)
#     print(t.get())

# 交换机类型
# celery.send_task('kwai.spider.video.comment', args=('3xzeaa9m92ypb4g',), exchange='kwai', exchange_type='direct')


# from celery.schedules import crontab


# @apps.on_after_configure.connect
# def setup_periodic_tasks(sender, **kwargs):
#     # 每30秒执行一次
#     sender.add_periodic_task(5, upper.s('abcdefg'), expires=10)  # 设置任务超时时间10秒
#
#     # 执行周期和Linux的计划任务crontab设置一样
#     sender.add_periodic_task(
#         crontab(hour='*', minute='*/2', day_of_week='*'),
#         add.s(11, 22),
#     )


# 消费者send_task
# celery_13_kuaishou.send_task('kwai.video.comment', kwargs={'data': rdata, 'post_time': int(time.time())},queue='kwai.video.comment')
