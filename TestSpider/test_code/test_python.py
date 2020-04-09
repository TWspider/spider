class Cat(object):
    def __init__(self):
        self.name = "Cat"

    def meow(self):
        return "meow!"


cat = Cat()

# __dict__
# 接口改变
# ex 适配器
cat.__dict__.update(dict(noise=cat.meow))
print(cat.noise())


# __getattr__
# 获取obj的属性值
# 适用于当前类的属性有另一个类,当前类没有此属性时,可去属性中的另一个类中找attr属性名
# ex 装饰器

def __getattr__(self, attr):
    """All non-adapted calls are passed to the object"""
    return getattr(self.obj, attr)


class foo(object):
    def f1(self):
        print("original f1")

    def f2(self):
        print("original f2")


class foo_decorator(object):
    def __init__(self, decoratee):
        self._decoratee = decoratee

    def f1(self):
        print("decorated f1")
        self._decoratee.f1()

    def __getattr__(self, name):
        return getattr(self._decoratee, name)


u = foo()
v = foo_decorator(u)
v.f1()
v.f2()


# __getitem__
# 获取值
def __getitem__(self, position):
    return self._cards[position]


# __enter__、__exit__
# 上下文管理

# __call__
# 可调用

# __len__、__getitem__、__setitem__、__delitem__、__contains__
# 集合

# 含attr
# 属性

# 迭代器iter

# map\filter\lambda
# map
# 适用于可迭代(func,iterable)
# 返回值
a = list(map(lambda x: x + 1, [1, 2, 3]))
print(a)

# lambda x:x+y, 1,2      # 左边参数:右边结果为返回值,传入参数
# 适用于一次性函数
a = lambda x, y: x + y
print(a(1, 2))

# filter
# 适用于可迭代(func,iterable)
# 返回过滤后的值
a = list(filter(lambda x: x % 2 == 1, [1, 2, 3]))
print(a)

# 元组拆包
# 元组作为(整体)本身(不可分)
a, *b = (1, 2, 3)
a = (1, 2,)
b = (3,)
c = a + b

import collections

# 创建简单对象
Card = collections.namedtuple('Card', ['rank', 'suit'])


class FrenchDeck:
    ranks = [str(n) for n in range(2, 11)] + list('JQKA')
    suits = 'spades diamonds clubs hearts'.split()

    def __init__(self):
        self._cards = [Card(rank, suit) for suit in self.suits
                       for rank in self.ranks]

    def __len__(self):
        return len(self._cards)

    def __getitem__(self, position):
        return self._cards[position]


print(FrenchDeck()[0])


# 装饰器
# 被修饰函数
def a(func):
    # 被修饰函数的参数x
    def do_something(x):
        print("开始运行")
        # 这里如果返回原函数,则进入原函数执行，其他则直接作为原函数返回值返回
        return func(5)

    # 将装饰器的闭包函数名,返回给原函数
    return do_something


# 执行顺序，先装饰器，在装饰器中进入原函数,最后将闭包中的返回值作为原函数的返回值

@a
def b(x):
    x = x - 2
    return x


print(b(5))


# 装饰器早期写法
def debug(func):
    def wrapper():
        print("[DEBUG]: enter {}()".format(func.__name__))
        return func()

    return wrapper


def say_hello():
    print("hello!")


say_hello = debug(say_hello)  # 添加功能并保持原函数名不变
say_hello()

# 叠加装饰器从上往下执行
# def d1():
#     print("d1")
#
# def d2():
#     print("d2")
#
# @d1
# @d2
# def f():
#     print(f)

# f = d1(d2(f))


# 开发者工具
# element
# console
# shift+enter 换行
# console要在点击要打印元素所属的页面才能生效
# 眼睛图标
# 动态跟踪输出
# __proto__
# 深色方法为可直接调用
# source
# 需要触发
# ctrl+p    搜索指定文件
# ctrl+g    输入:100 跳到100行
# ctrl+d    先选dom,高亮显示一样的dom
# 下一断点
# 直接获取到子函数的返回值
# 跳入函数
# 直接执行函数内余下部分,之后跳出函数
# 单步执行

# id(身份标识唯一的，也就是不能有两个相同的字符串，只能有两个不同的指针指向同一个字符串) is、type(数据类型)和value(值) ==

# 元组相对不可变,保存的是引用


# 不要使用可变类型作为参数的默认值
# 解决方法是在函数内部设置可变类型

# 弱引用:调用次数weakref.class ls(list) 列表和字典的子类可以弱引用

# python 有驻留特性

# property\staticmethod\classmethod
import math


class Circle:
    def __init__(self, radius):  # 圆的半径radius
        self.__radius = radius

    def get_area(self):
        return math.pi * self.__radius ** 2  # 计算面积

    def set_radius(self, radius):
        self.__radius = radius
        # return 2 * math.pi * self.__radius  # 计算周长

    def del_radius(self):
        raise TypeError('Can not delete')

    @staticmethod
    def stat(a, b):
        return a + b

    @classmethod
    def clas(cls, a, b):
        return a + b

    # 和直接加装饰效果一样
    # st = staticmethod(stat)
    # cl = classmethod(clas)
    x = property(get_area, set_radius, del_radius)


# 此时的特性不能被赋值\作用:统一获取、设置、删除的接口
c = Circle(10)
print("static")
print(Circle.clas(1, 3))


# c.x = 20
# print(c.x)


# staticmethod 实例也可用\作用:可以不赋值属性直接用类调用其中方法
# 静态方法第一个
# print(Circle.ts(1,2))


# classmethod
# 两者区别在于，类方法第一个参数要指向类对象，且如有属性则必须传参
# 两者共同处在于，都能被继承
class B(Circle):
    pass


print(B.stat(2, 3))
print(B(10).clas(2, 5))

# 默认字典、双向列队
from collections import defaultdict, deque

trig_lookup_table = defaultdict(list)
# 获取不存在的键时，返回默认字典内指定的值或类型,本次返回[].
print(trig_lookup_table["a"], "aaa")

# 生成器工具
import itertools

# 对生成器做切片操作
itertools.islice

# 1固定参数(非*args)2已缓存，运行速度快得多/键为参数，值为返回结果,闭包保存函数体/当参数太多时，且查询结果有序，则用二分法有序列表
# 结果用dict储存起来，查询.用内存换运算速度
# 生成器
# 字符串连接"".join()

# 异步
import asyncio


@asyncio.coroutine
def A():
    print('Hello, A!')
    # r 为none
    r = yield from asyncio.sleep(1)
    print('Hello again!')


@asyncio.coroutine
def B():
    print('Hello, B!')
    # r 为none
    r = yield from asyncio.sleep(1)
    print('Hello again!')


loop = asyncio.get_event_loop()  # 获取EventLoop的引用
tasks = [A(), B()]  # 把两个coroutine封装起来
loop.run_until_complete(asyncio.wait(tasks))  # 把封装好的coroutine放到EventLoop中执行
loop.close()

# 上下文管理
from contextlib import contextmanager


class A(object):
    def __enter__(self):
        print('__enter__() called')
        return self

    def print_hello(self):
        print("hello world")

    def __exit__(self, e_t, e_v, t_b):
        print('__exit__() called')


class A(object):

    def print_hello(self):
        print('hello world')


@contextmanager
def B():
    print('Start')
    a = A()
    yield a
    print('Over')


with B() as a:
    a.print_hello()

# 传参
# 集合作为参数
# querys = {
#     model.projectcaption
# }
# filters = {
#     model.region.like("%三元%")
# }
# result = session_model.query(*querys).filter(*filters).first()
# print(result.projectcaption)

# jit 编译

if __name__ == '__main__':
    import re
    pass
