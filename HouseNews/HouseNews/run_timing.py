import datetime

import redis
from scrapy import cmdline
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import sys

sys.path.append("C:\\Users\\zhangya6\\CODE")
from TW.spider.Test_spider.pafunction.house_tw.house_rank import mr, house_third


def job():
    os.system('scrapy crawl lianjia')
    os.system('scrapy crawl zhongyuan')
    os.system('scrapy crawl i5j')
    print('启动项目', datetime.datetime.now())
    os.system("python C:\\Users\\Administrator\\Desktop\\CODE\\TW\\spider\\HouseNews\\dafangya\\dafangya\\update_status.py")
    # 连接数据库，push起始url
    pool = redis.ConnectionPool(host="localhost", port=6379)
    conn = redis.Redis(connection_pool=pool, decode_responses=True)
    conn.lpush("myspider:start_urls", "https://www.dafangya.com")
    os.chdir("C:\\Users\\Administrator\\Desktop\\CODE\\TW\\spider\\HouseNews\\dafangya\\dafangya")
    print(os.getcwd())
    os.system("scrapy crawl dfy")
    conn.flushall()
    # 处理第三方房源http://pypi.douban.com/simple
    mr.match_rank(house_third)
    print('<<<<<<<<<<<<<<<<<<运行结束>>>>>>>>>>>>>>>>>')


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'interval', days=1, start_date="2019-12-16 00:00:00", misfire_grace_time=300, coalesce=True)
    scheduler.start()
    #
    # cmdline.execute('scrapy crawl lianjia -o lianjia.csv'.split())
    # test
    # cmdline.execute('scrapy crawl test'.split())
