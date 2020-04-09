import requests
import time
import re
import random
import json
from scrapy.selector import Selector
from selenium import webdriver
import sys
from selenium.webdriver.support.ui import WebDriverWait as Wait
from selenium.webdriver.support import expected_conditions as Expect
from selenium.webdriver.common.by import By
import numpy as np
from selenium.webdriver.common.action_chains import ActionChains


class TestRequest(object):
    def __init__(self):
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }
        self.current_path = sys.path[0]
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--ignore-certificate-errors')

    def start_request(self, url):
        '''
        Content-Length: 452
        :param url:
        :return:
        '''

        # cookie_jar = requests.cookies.RequestsCookieJar()
        '''
        domain=sh; expires=Thu, 12-Dec-2019 04:59:03 GMT; Max-Age=3600; path=/; domain=5i5j.com
        '''

        # s.cookies.set('mycookie', 'value')
        # response = s.get(url=url, headers=headers)
        # print(response.text)
        browser = webdriver.Chrome(executable_path="{}\chromedriver.exe".format(self.current_path),
                                   chrome_options=self.options)
        # browser.maximize_window()
        browser.delete_all_cookies()
        browser.get(url)
        while True:
            r = random.randint(1, 3)
            print(r)
            time.sleep(r)
            try:
                # element = browser.find_element_by_xpath('//canvas[@class="dvc-captcha__canvas"]')
                # element1 = browser.find_element_by_xpath('//div[@class="dvc-operate"]')
                # ActionChains(browser).click_and_hold(element).move_by_offset(200, 300).release().perform()
                pass
                Wait(browser, 5).until(
                    Expect.presence_of_element_located((By.XPATH, "//div[@class='zu-info']/h3/a/b[@class='strongbox']"))
                )
                # 获取当前页
                html = browser.page_source
                res = Selector(text=html)
                ls = res.xpath("//div[@class='house-title']/a").extract()
                print(len(ls), ls)
                # 进入下一页
                browser.find_element_by_xpath("//a[@class='aNxt']").click()
            except Exception as e:
                print(e)
                break
        browser.quit()

    def page_list(self, s, url_flag, url):
        '''
        proxies={'https': 'https://117.169.78.54:18039'}
        :param s:
        :param url:
        :return:
        '''
        r = random.randint(1, 3)
        print(r)
        time.sleep(r)
        if not url_flag:
            url = url
        response = s.get(url=url, headers=self.headers
                         )
        res = Selector(response=response)
        project_list = res.xpath(
            "//div[@class='zu-itemmod']/div[@class='zu-info']/h3/a/b[@class='strongbox']").extract()
        print(len(project_list), project_list)
        next_page = res.xpath("//a[@class='aNxt']/@href").extract_first()
        if next_page:
            self.page_list(s=s, url_flag=True, url=next_page)
        else:
            # print(response.text)
            pass

    def page_detail(self, s):
        url = "https://shanghai.anjuke.com/prop/view/A1945068584?from=filter&spread=commsearch_p&uniqid=pc5e05a89b974f83.24365657&position=1&kwtype=filter&now_time=1577429147"
        response = s.get(url=url, headers=self.headers)
        res = Selector(response=response)
        title = res.xpath("//h3[@class='long-title']/text()").extract_first()
        print(title)

    def verify_code(self, s):
        '''
        callback=shield&from=antispam&serialID=505ad2493af6321f2345ce63677568a3_116975fa15f04674aa92745d7f579da2&history=aHR0cDovL3NoLmZhbmcuYW5qdWtlLmNvbS8%2FZnJvbT1uYXZpZ2F0aW9u
        callback=shield&from=antispam&namespace=anjuke_c_pc&serialID=db45274de43827796d574ea478180d6d_27fc714a9e524ee79611c94b766677d4&history=aHR0cHM6Ly9zaGFuZ2hhaS5h
        从原网页得到sessionid，跳转
        通过此：https://verifycode.58.com/captcha/getV3?callback=jQuery110107363398125827825_1577674941241&showType=win&sessionId=59ec14d161bb41069ef7ce9bddf5b69b&_=1577674941242
        得到rid
        请求得到验证码
        :param s:
        :return:
        '''
        url1 = "https://sh.fang.anjuke.com/xinfang/captchaxf-verify/?callback=shield&from=antispam&serialID=db704c0bdcafaae27335480136f48d2f_4413994fff534403a5815b6e76b18967&history=aHR0cDovL3NoLmZhbmcuYW5qdWtlLmNvbS8%2FZnJvbT1uYXZpZ2F0aW9u"
        response1 = s.get(url=url1, headers=self.headers)
        res1 = Selector(response=response1)
        sessionId = res1.xpath("//input[@name='sessionId']/@value").extract_first()
        t = time.time()
        url2 = "https://verifycode.58.com/captcha/getV3?callback=jQuery19105087918389780279_1577675951673&showType=embed&sessionId={}&_={}".format(
            sessionId, t)
        response2 = s.get(url=url2, headers=self.headers)
        rid = re.search('"responseId":"(.*?)"', response2.text).group(1)
        url3 = "https://verifycode.58.com/captcha/captcha_img?rid={}&it=_big".format(rid)
        print(url3)
        response3 = s.get(url=url3, headers=self.headers)
        rnum = random.random()
        with open('code.png', 'wb') as f:
            f.write(response3.content)
        print(">>>>>>图片保存成功!<<<<<<<")

        # data = {
        #     "responseId": "",
        #     "sessionId": "",
        #     "data": ""
        # }
        # response = s.post(url=url, data=data)
        # print(response)

    def verify_img(self):

        import cv2
        rnum = random.random()
        img = cv2.imread('background.png')

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # cv2.imshow("gray", gray)

        ret, binary = cv2.threshold(gray, 127, 128, cv2.THRESH_BINARY)
        contours, hierarchy = cv2.findContours(binary, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        cv2.drawContours(img, contours, -1, (0, 160, 255), 3)
        cv2.imshow("img", img)
        cv2.waitKey(0)

        # 读取截图
        # screenshot = cv2.imread('code.png')
        # # 筛选出符合颜色区间的区域
        # inRange = cv2.inRange(screenshot, np.array([90, 90, 90]), np.array([115, 115, 115]))
        # # 从图中找出所有的轮廓
        # _, cnts, _ = cv2.findContours(inRange.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        # # 对所有轮廓做排序，排序依据是每个轮廓包含的点的数量
        # cnts.sort(key=len, reverse=True)
        #
        # # 取前两个轮廓(有些图片目标位置不一定是第一个轮廓)
        # for cnt in cnts[0: 2]:
        #     xSum = 0
        #     xCounter = 0
        #     for position in cnt:
        #         xCounter += 1
        #         xSum += position[0][0]
        #     # 算出所有点的X坐标平均值，并在此基础上做一个60像素的偏移，这个偏移可以根据自己手机进行调整
        #     x = int(xSum / xCounter - 20)
        #     # 在截图上画一条红线，表示识别的x坐标位置
        #     cv2.line(screenshot, (x, 0), (x, 500), (0, 0, 255), 5)
        # cv2.imshow("screenshot", screenshot)
        # cv2.waitKey(0)

    def m_request(self):
        base_url = "https://m.anjuke.com/sh/sale/pudong/?from=anjuke_home&page={}"
        for i in range(1, 50):
            url = base_url.format(str(i))
            response = requests.get(url=url, headers=self.headers)
            res = Selector(response=response)
            title_list = res.xpath('//div[contains(@class,house-title)]/text()').extract()
            print(len(title_list), title_list)


if __name__ == '__main__':
    '''
    二手房
    https://m.anjuke.com/sh/sale/?from=anjuke_home
    租房
    https://m.anjuke.com/sh/zf/?from=anjuke_home
    '''
    rh = TestRequest()
    time_start = time.time()
    s = requests.session()
    # 租房
    # url = 'https://sh.zu.anjuke.com/?from=navigation'
    # 二手房
    url = "https://shanghai.anjuke.com/sale/?from=navigation"
    # rh.page_list(s=s, url_flag=False, url=url)
    # rh.start_request(url=url)
    rh.m_request()

    # rh.verify_code(s=s)
    # rh.verify_img()
    # for i in range(1000):
    # rh.verify_code(s=s)

    #     rh.page_list(s=s,url=None)
    #     rh.page_detail(s=s)
    time_end = time.time()
    # 运行时间
    print(time_end - time_start)
