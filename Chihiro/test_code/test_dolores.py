import pandas as pd
import difflib
import jieba
from sqlalchemy import create_engine
import time
import re
import numpy as np

time_start = time.time()


# 一星

class T(object):
    def __init__(self):
        pass


    def a(self):

        self.s(0, "a")
        self.s(0, "b")
        self.s(0, "c")


    def s(self, i, flag):
        if flag == "a":
            print("a")
        elif flag == "b":
            print("b")
        else:
            print("c")
        if i >= 3:
            return
        else:
            i += 1
            self.s(i, flag)

time_end = time.time()
print('totally cost', time_end - time_start)
