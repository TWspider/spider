import os
import logging
from twisted.internet import reactor
from apscheduler.schedulers.twisted import TwistedScheduler

# import asyncio
# from twisted.internet import asyncioreactor
#
# asyncioreactor.install(asyncio.get_event_loop())
# 内置执行模块不能执行多个爬虫
# from scrapy.cmdline import execute

# 主模块不能用相对导入
# from house_rank.house_rank import mr

if __name__ == '__main__':
    scheduler = TwistedScheduler()


    def start_1():
        spider_list_1 = [
            "lianjia_esf",
            "lianjia_zf",
            "i5j_esf",
            "i5j_zf",
            "zhongyuan_esf",
            "zhongyuan_zf",
            "dafangya",
            "xinyi_esf",
            "xinyi_zf",
        ]
        for spider1 in spider_list_1:
            try:
                os.system('scrapy crawl {}'.format(spider1))
            except Exception as e:
                logging.info(e)
                logging.info("异常爬虫:{}".format(spider1))
                print(e)
                print("异常爬虫:{}".format(spider1))


    def start_2():
        spider_list_2 = [
            "anjuke_esf",
            "anjuke_zf",
        ]
        for spider2 in spider_list_2:
            try:
                os.system('scrapy crawl {}'.format(spider2))
            except Exception as e:
                logging.info(e)
                logging.info("异常爬虫:{}".format(spider2))
                print(e)
                print("异常爬虫:{}".format(spider2))

    scheduler.add_job(start_1, 'interval', days=1, start_date='2020-03-24 06:00:00', misfire_grace_time=10)
    scheduler.add_job(start_2, 'interval', days=5, start_date='2020-03-24 06:00:00', misfire_grace_time=10)
    scheduler.daemonic = False
    scheduler.start()
    try:
        # start_1()
        # start_2()
        reactor.run()
    except (KeyboardInterrupt, SystemExit):
        pass
