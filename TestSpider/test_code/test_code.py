from concurrent import futures
from twisted.internet import reactor
from selenium import webdriver
import time
import random


def hello():
    from time import sleep
    # 1.创建Chrome浏览器对象，这会在电脑上在打开一个浏览器窗口
    browser = webdriver.Chrome(executable_path="D:\My project\TW\spider\Chihiro\\test_code\chromedriver.exe")

    # 2.通过浏览器向服务器发送URL请求
    browser.get("https://sh.5i5j.com/ershoufang/")
    cookies = browser.get_cookies()
    cookies_str = ""
    for i in cookies:
        k = i.get("name")
        v = i.get("value")
        cookies_str += k+"="+v+";"
    print(cookies_str)
    browser.quit()


if __name__ == '__main__':
    # from datetime import datetime
    # t1 = 1576735854
    # t2 = 1608271854
    # dt1 = datetime.utcfromtimestamp(t1)
    # dt2 = datetime.utcfromtimestamp(t2)
    # res = (dt2 - dt1).seconds
    # print(res)
    # hello()
    import time
    import asyncio
    import sys
    p = sys.path[0]
    print(p,type(p))


    async def _sleep(x):
        time.sleep(2)
        return '暂停了{}秒！'.format(x)


    def callback(future):
        print('这里是回调函数，获取返回结果是：', future.result())

    def callback1(future):
        print('这里是回调函数，获取返回结果是：', future.result())

    coroutine = _sleep(2)
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(coroutine)

    # 添加回调函数
    task.add_done_callback(callback)
    task.add_done_callback(callback1)

    loop.run_until_complete(task)
