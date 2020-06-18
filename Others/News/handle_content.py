import re
from scrapy.selector import Selector


UNRELATED_KEYWORD ='以上推荐为优质及原创文章|篱笆社区|分享新浪微博|引用只看此人|下一页第页确定|注册日期|第\d+楼|被.+编辑过|当前微信版本不支持该功能，请升级至最新版本。'

def req_content(res_text):
    res_xml = Selector(text=res_text)
    res = res_xml.xpath("//div").xpath("string(.)").extract()
    res_line = []
    for i in res:
        res_clean = re.sub(r"\s+", '\n', i)

        res_clean = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fa5\.%，。！？：,.!?:]+", '', res_clean)
        res_split = []
        for item in res_clean.split('\n'):
            if len(item) < 10:
                continue

            if re.search(UNRELATED_KEYWORD, item):
                continue

            if re.search('[\u4e00-\u9fa5]', item):
                res_split.append(item)
        res_line.extend(res_split)
    res_line = set(res_line)
    content = '<SEP>'.join(res_line)
    return content

if __name__ == "__main__":
    pass
    # req_content()