from scrapy import cmdline

if __name__ == '__main__':
    # cmdline.execute('scrapy crawl lianjia'.split())
    cmdline.execute('scrapy crawl zhongyuan'.split())
    # cmdline.execute('scrapy crawl i5j'.split())

    # cmdline.execute('scrapy crawl lianjia -o lianjia.csv'.split())
    # test
    # cmdline.execute('scrapy crawl test'.split())
