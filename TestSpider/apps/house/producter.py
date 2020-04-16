from Chihiro.apps.house.app_house import app
# 起始 worker;测试用test_page、test_item
START_TASK = "page_1"
START_URL = ("https://sh.centanet.com/ershoufang/",)
QUEUE = "test_page"

app.send_task(START_TASK, args=START_URL, queue=QUEUE)