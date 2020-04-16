from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Executor
import time

t = 1


def test(a, b):
    global t
    time.sleep(3)
    t += 1
    print(t)
    print(a, b)

def test1(a, b):
    global t
    time.sleep(3)
    t += 1
    print(t)
    print(a, b)


with ThreadPoolExecutor(max_workers=10) as executor:
    future = executor.submit(test, 323, 1235)
    future1 = executor.submit(test1, 323, 1235)
    print(1)
    # print(future.result())
    print(222)
