import requests
import time

s = requests.session()
s.keep_alive = False

headers = {
    'Cookie': "browsermark=hxkuj3v9+uyEoemK3ZuyrQ==; lang=zh; Authorization=5Uom2nogzz8+QHhyINTOBA=="
}

start_time_input = '2020-6-02 00:00:00'
start_time_input = time.strptime(start_time_input, "%Y-%m-%d %H:%M:%S")  # 定义格式
start_time = int(time.mktime(start_time_input))

end_time_input = '2020-6-03 00:00:00'
end_time_input = time.strptime(end_time_input, "%Y-%m-%d %H:%M:%S")  # 定义格式
end_time = int(time.mktime(end_time_input))
print(start_time)
print(end_time)
# start_time = 1591027200
# end_time = 1591027200
url = 'http://172.25.161.190/service/record/~/time[{start_time},{end_time}]/'.format(start_time=start_time,
                                                                                     end_time=end_time)
print(url)
response = s.get(
    url,
    headers=headers)
res_json = response.json()
res_ls = [url.get("playfile") for url in res_json if url.get("playfile") != None]
for i in res_ls:
    fn = i.strip('/record/').strip('.wav').replace("/", '_')
    url = 'http://172.25.161.190/playfile_stream.wav?filename=./www{}&ch=0'.format(i)
    print(fn)
    print(url)
    headers = {
        'Cookie': 'browsermark=hxkuj3v9+uyEoemK3ZuyrQ==; lang=zh; Authorization=5Uom2nogzz8+QHhyINTOBA=='
    }
    req = s.get(url, headers=headers)
    with open('./{}.wav'.format(fn), mode='wb+') as f:
        f.write(req.content)
