import asyncio
import time
import aiohttp


async def get_html(url):
    print(">>>start<<<")
    await asyncio.sleep(2)
    print(">>>end<<<")
    return "bobby"


async def downer():
    time.sleep(2)
    print(111)
    # return "1g1g1g"


async def down_html():
    html = await downer()
    print(222)
    # return html


# print(down_html().send(None))

if __name__ == '__main__':
    start_time = time.time()


    def a():
        # time.sleep(1)
        yield "c"
        yield "d"
        yield "e"


    # def b():
    #     # time.sleep(2)
    #     # print("b")
    #     yield "b"

    b = 1
    def ts():
        global b
        b = b+1
        print(b)
        def ts1():
            print(b)
        ts1()
    ts()
    # print(s.send(2))

    # b().send(None)
    # loop = asyncio.get_event_loop()
    # task = [get_html("www.baidu.com") for i in range(10)]
    # loop.run_until_complete(asyncio.wait(task))
    # print(task)
    # print(time.time() - start_time)
