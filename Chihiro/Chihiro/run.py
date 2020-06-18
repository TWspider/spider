import os
import logging
from twisted.internet import reactor
from apscheduler.schedulers.twisted import TwistedScheduler

# 内置执行模块不能执行多个爬虫
from scrapy.cmdline import execute

# 主模块不能用相对导入
# from house_rank.house_rank import mr

if __name__ == '__main__':
    scheduler = TwistedScheduler()


    def scheduler_house():
        spider_list_1 = [
            "lianjia_esf",
            "lianjia_zf",
            "zhongyuan_esf",
            "zhongyuan_zf",
            "i5j_esf",
            "i5j_zf",
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

    def scheduler_house_anjuke():
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

    def scheduler_community():
        spider_list_3 = [
            "community_lianjia",
            "community_zhongyuan",
            "community_i5j"
        ]
        for spider3 in spider_list_3:
            try:
                os.system('scrapy crawl {}'.format(spider3))
            except Exception as e:
                logging.info(e)
                logging.info("异常爬虫:{}".format(spider3))
                print(e)
                print("异常爬虫:{}".format(spider3))

    def scheduler_start():
        scheduler.add_job(scheduler_house, 'interval', days=3, start_date='2020-03-24 06:00:00', misfire_grace_time=10)
        scheduler.add_job(scheduler_house_anjuke, 'interval', days=5, start_date='2020-03-24 06:00:00', misfire_grace_time=10)
        scheduler.add_job(scheduler_community, 'interval', days=30, start_date='2020-05-01 00:00:00', misfire_grace_time=20)
        scheduler.daemonic = False
        scheduler.start()
        reactor.run()
    def test_spider():
        spider_list = [
            "test",
        ]
        for spider in spider_list:
            try:
                os.system('scrapy crawl {}'.format(spider))
            except Exception as e:
                logging.info(e)
                logging.info("异常爬虫:{}".format(spider))
                print(e)
                print("异常爬虫:{}".format(spider))
    try:
        scheduler_start()
        # scheduler_community()
        # scheduler_house()
        # scheduler_house_anjuke()
        # test_spider()
    except (KeyboardInterrupt, SystemExit):
        pass
        print("test")
