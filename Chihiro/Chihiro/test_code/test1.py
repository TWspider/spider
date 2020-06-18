# coding = utf-8
from selenium import webdriver
import execjs
import time
from selenium.webdriver.common.action_chains import ActionChains

browser = webdriver.Chrome()

browser.get("http://www.twrsp.com")

# 点击到登陆界面
span = browser.find_element_by_class_name('login-button')
span.click()

# 输入用户名
browser.find_element_by_id("txtUserName").send_keys("xuxiang")

# 输入密码
browser.find_element_by_id("txtUserPW").send_keys("Zxcvb12345")

# 登陆

ss = browser.find_element_by_xpath("//*[@class='loginMode']//button")

ss.click()

# 进入菜单
time.sleep(3)
browser.find_element_by_id("menuLike-00377").click()
print('登陆成功')

time.sleep(1)
browser.find_element_by_xpath("//*[@data-menucode='00377-00381']").click()
print('进入业务中心成功')

time.sleep(1)
browser.find_element_by_xpath("//*[@data-menucode='00381-00383']").click()

print('进入房源列表成功')
time.sleep(3)

ss = browser.find_element_by_xpath("//div[contains(@class,'hm-pointer')]/span[contains(@class,'hm-estateNameStyle')]")
ActionChains(browser).double_click(ss).perform()
print('打开房源详情页')

# time.sleep(5)

# execjs.eval("alert('sss')")

# print(span.get_attribute(name='login-input'))


