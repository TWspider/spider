import asyncio
from pyppeteer import launch
from scrapy.selector import Selector
import requests
import re


async def baidu():
    browser = await launch(headless=False)
    page = await browser.newPage()

    start_url = '''
        https://www.baidu.com/s?wd=python%E5%AD%97%E7%AC%A6%E4%B8%B2%E8%BD%ACurl&rsv_spt=1&rsv_iqid=0xdf78155d00148ab5&issp=1&f=8&rsv_bp=1&rsv_idx=2&ie=utf-8&rqlang=cn&tn=baiduhome_pg&rsv_enter=1&rsv_dl=tb&inputT=1103&rsv_t=4d64XyGSTAB76hhLAFKtZEckpoj2m9NJe8TSNbNDotZty3LRRrenn9eGo2rg4tbwq%2Bpa&rsv_sug3=33&oq=%25E5%25AD%2597%25E7%25AC%25A6%25E4%25B8%25B2%25E8%25BD%25ACurl&rsv_pq=dc608ecc0007dc75&rsv_sug2=0&rsv_sug4=1711    '''
    await page.setExtraHTTPHeaders(
        {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'PSTM=1578018566; BIDUPSID=6FBF449654651402107372BC91E5F0C3; BAIDUID=E98C3D74DBEF325A1C9073AA1BADC7EF:FG=1; BDUSS=XpYWFczTDRGb0UxaWx5Tmt0emZocWZOb1FDclAtRzFVem5XSWhMLU00MnpLajVlSVFBQUFBJCQAAAAAAAAAAAEAAACnVEtHx6fdobqjAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALOdFl6znRZeZ; BD_UPN=12314753; BDSFRCVID=61KsJeCCxG3HtqTuE1nDsJ5EP34iX_Jc8b7w3J; H_BDCLCKID_SF=tJKj_CDMJDv5j5uGMjo8q4tehHnj245d55ryL4tBKJjEe-KIK-ctBPu8bMrt26rj-TQD8tJE5f72DbjkHDoqjDLVjgoKtDQ-JPTa065qJ55qM65pDK_WjIL8eeoKJDL8fPcjWRQ7bRrEfKCNbjDq-jO8eat-K6F-KRu-VCPD-n6t_nvN2JJoK43KJf_qD4_jjR-f8RKXjRDK2DI9JR3Vbl4_-Uby-46JqRCfVCvvKlb5OR5JLn7nDPQ-DGbt0lQRyeQZan6M3UcUeRTthUrkQPTyyGCDt5FHJRCtV-35b5rHDRcGMP__-P4DeNDDBbJZ5m7mXp0bWPbaS-5HDtTUKUT0QJ0etPcvX2Q2Vn_5bKOkbRO4-TFKDjQXjx5; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; H_WISE_SIDS=142081_142696_142115_141693_142206_135847_141001_142427_142315_142067_142018_141838_140631_140988_141898_142397_142286_140578_133847_140065_134046_141808_131423_141918_125580_142001_107319_141964_140368_137782_140965_136413_110085_142271_141860_142345_140853_138878_137985_136862_140174_131247_137749_138165_138883_140259_141941_127969_140593_138426_141191_141924; delPer=0; BD_CK_SAM=1; PSINO=5; COOKIE_SESSION=4140_0_9_6_2_12_0_3_9_6_1_6_13496_0_1_0_1582700865_0_1582700864%7C9%23228677_23_1582262245%7C6; H_PS_PSSID=30744_1440_21083_30791_30824_26350; sug=3; sugstore=0; ORIGIN=2; bdime=0; H_PS_645EC=cc0fZNtS%2FYzVM69skSxdoLRwfmQRuYQzul21DU1FSD%2BKBX%2BuUOAbPUkH8h9Ds4LnHd09; BDSVRTM=0',
            'Host': 'www.baidu.com',
            'Referer': 'https://www.baidu.com/s?ie=utf-8&f=3&rsv_bp=1&rsv_idx=2&tn=baiduhome_pg&wd=%E9%87%91%E6%B8%90%E5%B1%82&rsv_spt=1&oq=_mssql.MSSQLDatabase%2526gt%253Bxception%253A%2520(1205%252C%2520b%2526%252339%253BTransaction%2520(Process%2520ID%2520%2526lt%253B57)%2520was%2520dea&rsv_pq=8d1eea3c00092231&rsv_t=f629fs16AW6jR28n97Qd8LXpbHV8BomsWCie9UM4bH1KbiJH%2B7nSk1FK1DAR%2BXfwJLwI&rqlang=cn&rsv_enter=1&rsv_dl=ts_1&inputT=6739&rsv_sug3=124&rsv_sug1=63&rsv_sug7=100&rsv_sug2=0&prefixsug=%25E9%2587%2591%25E6%25B8%2590&rsp=1&rsv_sug4=7192',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36',
        }
    )
    await page.goto(start_url)
    # script = "Object.defineProperty(navigator,'webdriver', {get:()=>false,});"
    cont = await page.content()
    res_cont = Selector(text=cont)
    url_list = res_cont.xpath("//div[@id='content_left']/div/h3[@class='t']/a/@href")
    for url in url_list:
        # url = re.search("link?(.*?)").group(1)
        # tar = "https://mp.weixin.qq.com/link?" + url
        url = url.extract()
        print(url)
        res = requests.get(url)
    await browser.close()


async def sougou():
    browser = await launch(headless=True)
    page = await browser.newPage()
    start_url = 'https://weixin.sogou.com/weixin?type=2&query=%E8%8F%81%E8%8B%B1%E5%9C%B0%E4%BA%A7'
    await page.setExtraHTTPHeaders(
        {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'weixin.sogou.com',
            'Referer': start_url,
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36',
        }
    )
    await page.goto(start_url)
    # script = "Object.defineProperty(navigator,'webdriver', {get:()=>false,});"
    cont = await page.content()
    print(cont)
    res_cont = Selector(text=cont)
    url_list = res_cont.xpath("//div[@class='txt-box']/h3/a/@href")
    for url in url_list:
        # url = re.search("link?(.*?)").group(1)

        url = url.extract()
        tar = "https://mp.weixin.qq.com" + url
        print(tar)
        await page.goto(tar)

        # print(url)
        # res = requests.get(url)
        # print("链接是")
        # print(res.text)
    await browser.close()





if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(sougou())
    # asyncio.get_event_loop().run_until_complete(baidu())
