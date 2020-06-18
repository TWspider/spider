FROM selenium/standalone-chrome

USER root
COPY requirement.txt /
WORKDIR /

RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
apt-get clean && \
apt-get update && \
apt-get install -y software-properties-common && \
apt-get install -y python3.6 && \
apt-get install -y python3-pip && \
pip3 install -r requirement.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com

# 直接写入volume会报错no such
#VOLUME . /TestCode
WORKDIR /test_code/Chihiro

#WORKDIR /code
#CMD python run.py

#VOLUME . /code
#WORKDIR /code/Chihiro
#CMD python run.py