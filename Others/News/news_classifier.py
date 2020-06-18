import pymssql
import re
from collections import namedtuple


News = namedtuple(
    'News',
    [
        'title',
        'content'
    ]
)

CONN = pymssql.connect(
    '10.10.202.13',
    'bigdata_user',
    'ulyhx3rxqhtw',
    'TWSpider',
)


STOP = '[,，。?!，．？！]'
SPLIT = r'。|！|!|？|\?|！|!|；|;|\||\s{2,}'
with open('config/synonymous.txt', 'r', errors='ignore') as fp:
    SYNONYMOUS = [item.split(',') for item in fp.read().split('\n') if item]
with open('config/regular.txt', 'r', errors='ignore') as fp:
    REGULAR = fp.read().split('\n')
# print(REGULAR)
def disambiguation(string):
    string = string.upper()
    for a, b in SYNONYMOUS:
        string = re.sub(a, b, string)

    return string


def preprocess(title, content):
    lines = []

    """文本拆分"""
    if title:   lines.extend(re.split(SPLIT, title))
    if content: lines.extend(re.split(SPLIT, content))

    """消歧并去除空字符串"""
    lines = [disambiguation(item) for item in lines if item]

    """去重并保存"""
    lines = [item for item in set(lines) if item]

    """相关文本"""
    lines = [item for item in lines if re.search('太平洋', item)]

    return lines


def detect_line(line):
    for idx, reg in enumerate(REGULAR, 1):
        if re.search(reg, line):
            return idx

    return False


def news_classifier(title, content):

    lines = preprocess(title, content)
    for line in lines:
        if detect_line(line):
            return True

    return False


def get_news():
    cursor = CONN.cursor(as_dict=True)
    sql = """
    SELECT title,
           content
      FROM dbo.content
    """

    cursor.execute(sql)
    news_list = cursor.fetchall()
    news_list = [News(item['title'], item['content']) for item in news_list]

    return news_list


def test_news_analysis():
    news_list = get_news()

    result = []
    for news in news_list:
        result.append((int(news_classifier(news.title, news.content)), news.title))

    print(1)

    # update_sql = """
    # UPDATE dbo.content
    #    SET flag_artificial = %s
    #  WHERE title = %s
    # """

    # cursor = CONN.cursor()
    # cursor.executemany(
    #     update_sql,
    #     result
    # )


def test_detect_line():
    with open('config/negative.txt', 'r', errors='ignore') as fp:
        negative = fp.read().split('\n')

    negative = [item.replace(' ', '') for item in negative]

    remain = []
    for line in negative:
        if not detect_line(line):
            remain.append(line)

    print(1)


if __name__ == "__main__":
    test_news_analysis()
    test_detect_line()


