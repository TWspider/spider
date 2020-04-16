# import datetime
# import os
# import redis
# from apscheduler.schedulers.blocking import BlockingScheduler
#
#
# # 设置 scrapy 运行环境
# # BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# # sys.path.insert(0, BASE_DIR)
# # os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "dafangya.dafangya.settings")
#
# def job():
#     # 启动项目
#     print('启动项目', datetime.datetime.now())
#     os.system("python update_status.py")
#     # 连接数据库，push起始url
#     pool = redis.ConnectionPool(host="localhost", port=6379)
#     conn = redis.Redis(connection_pool=pool, decode_responses=True)
#     conn.lpush("myspider:start_urls", "https://www.dafangya.com")
#     os.system("scrapy crawl dfy")
# conn.fluall()
#     # 处理第三方房源
#
#
# if __name__ == '__main__':
#     scheduler = BlockingScheduler()
#     scheduler.add_job(job, 'interval', days=1, start_date="2019-12-18 10:01:00")
#     scheduler.start()
