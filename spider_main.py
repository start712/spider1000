# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: spider_main.py
    @time: 2017/6/1 14:50
--------------------------------
"""
import codecs
import csv
import re
import sys
import os
import traceback

import bs4
import numpy
import pandas as pd
import requests
import json

import time

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('spider_main.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('spider_main.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

key_dict = {
    u'宗地坐落':'parcel_location',
    u'宗地编号':'parcel_no',
    u'宗地面积':'offer_area_m2',
    u'容积率':'plot_ratio',
    u'土地用途':'purpose',
    u'起始价':'starting_price_sum',
    u'地块编号':'parcel_no',
    u'地块位置':'parcel_location',
    u'成交价(万元)':'transaction_price_sum'
}

class spider_main(object):
    def __init__(self):
        self.urls = ["http://www.zjdlr.gov.cn/col/col1071192/index.html?uid=4228212&pageNum=%s" %i for i in xrange(911) if i > 0]

    def get_titles(self, url):
        print u"\n新的公告页------------------>读取%s中的公告标题 \n" %url
        resp = requests.get(url)
        resp.encoding = 'utf8'
        s_code = resp.status_code
        if s_code != 200:
            log_obj.debug("打开url:%s失败" %url)
        else:
            root_site = "http://www.zjdlr.gov.cn"
            rows = re.findall(r"(?<=<record><!\[CDATA\[).*?(?=</record>)", resp.text, re.S)

            for row in rows:
                if row:
                    try:
                        monitor_title = re.search(r"(?<=title=').*?(?=' target=)", row).group(0) # 出让公告标题
                        monitor_date = re.search(r'(?<=class="bt_time" style="font-size:16px;border-bottom:dashed 1px #ccc">).*?(?=</td>)', row).group(0) # 发布日期
                        monitor_url = root_site + re.search(r"(?<=href=').*?(?=' class)", row).group(0) # 链接

                        print u"输出%s中的公告：（%s,%s,%s）" %(url,monitor_title, monitor_date, monitor_url)
                        yield monitor_title, monitor_date, monitor_url
                    except:
                        info = sys.exc_info()
                        log_obj.debug(u"存在无法解析的xpath：%s\n原因：%s%s%s" %(row, info[0], ":", info[1]))


    def parse_page(self, monitor_title, monitor_date, monitor_url):
        time.sleep(3)
        print u"正准备解析%s" %monitor_url
        resp = requests.get(monitor_url)
        resp.encoding = 'utf8'
        s_code = resp.status_code
        if s_code != 200:
            log_obj.debug("打开url:%s失败" %monitor_url)
        else:
            bs_obj = bs4.BeautifulSoup(resp.text, 'html.parser')
            parcel_status = 'onsell'
            sites = bs_obj.find_all('table', style='border-collapse:collapse; border-color:#333333;font-size:12px;')

            try:
                for site in sites:
                    parcel_no = re.search(r'(?<=\().*(?=\))', monitor_title).group()
                    content_detail = {'addition': {}}

                    if not site:
                        log_obj.debug(u"%s没有检测到更多detail" % resp.url)

                    data_frame = pd.read_html(str(site), encoding='utf8')[0]  # 1
                    data_frame = data_frame.fillna('')  # 替换缺失值
                    col_count = len(data_frame.columns)
                    if col_count % 2 == 0:
                        # 一列标题，下一列为数据
                        # 先将数据线data frame数据转化为numpy数组，然后将数组reshape改成2列
                        arr = numpy.reshape(numpy.array(data_frame), (-1, 2))
                        # 去除key中的空格和冒号
                        data_dict = dict(arr)
                        r = re.compile(ur'\s+|:|：')
                        data_dict = {r.sub('', key): data_dict[key] for key in data_dict if
                                     (type(key) == type(u'') or type(key) == type('')) and key != 'nan'}
                        for key in data_dict:
                            if key in key_dict:
                                # key_dict[key]将中文键名改成英文的
                                content_detail[key_dict[key]] = data_dict[key]
                                #print "%s:%s" %(key_dict[key], data_dict[key])
                            else:
                                content_detail['addition'][key] = data_dict[key]
                    print u"输出%s中的%s数据.............." %(monitor_url, parcel_no)
                    #content_detail = json.dumps(content_detail).encode('utf8')
                    yield monitor_title, monitor_date, monitor_url, parcel_no, content_detail
            except:
                log_obj.error("%s中无法解析%s\n%s" % (resp.url, monitor_title, traceback.format_exc().decode('gbk').encode('utf8')))

    def main(self):
        try:
            with open('datail.csv', 'a') as csvfile:
                csvfile.write(codecs.BOM_UTF8)  # 防止乱码
                writer = csv.writer(csvfile)
                writer.writerow(['标题', '日期', 'url', '地块编号（主）', '地块编号（副）', '地块位置', '宗地面积',
                                 '土地用途', '容积率', '起始价', '成交价(万元)', '其他'])

            for url in self.urls:
                for pack in self.get_titles(url):
                    if pack:
                        monitor_title, monitor_date, monitor_url = pack
                    else:
                        continue

                    data_generator = self.parse_page(monitor_title, monitor_date, monitor_url)
                    if data_generator:
                        for data0 in data_generator:
                            """
                            with open("data.txt", 'a') as f:
                                input = "%s,%s,%s,%s,%s\n" %data0
                                f.write(input)
                            """
                            parcel_no_main = data0[3]
                            content_detail = data0[4]
                            for key in key_dict.viewvalues():
                                if key not in content_detail:
                                    content_detail[key] = ''

                            addition = ' ; '.join([':'.join(t) for t in content_detail['addition'].items()])
                            with open('datail.csv', 'a') as csvfile:
                                csvfile.write(codecs.BOM_UTF8)  # 防止乱码
                                writer = csv.writer(csvfile)
                                writer.writerow([monitor_title, monitor_date, monitor_url, parcel_no_main,content_detail['parcel_no'],
                                                content_detail['parcel_location'], content_detail['offer_area_m2'], content_detail['purpose'],
                                                content_detail['plot_ratio'], content_detail['starting_price_sum'], content_detail['transaction_price_sum'],
                                                addition])
        except:
            log_obj('%s' %traceback.format_exc())


if __name__ == '__main__':
    spider_main = spider_main()
    spider_main.main()