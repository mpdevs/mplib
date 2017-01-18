#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
from reg_abc import reg_abc

def unit_test(regexp_para,test_case):
    # 正则实现
    regexp_out = reg_abc(regexp_para)
    print regexp_out
    for line in test_case:
        sample_text, r = line
        # print sample_text
        try:
            if re.search(regexp_out, sample_text) is None:
                rr = False
            else:
                rr = True
            if rr != r:
                print sample_text, r
        except Exception as e:
            # print e
            print "reg_abc怎么还有bug？"
if __name__ == "__main__":
    # 字典结构：{中心词(值、距离、逻辑)，前缀词(值、距离、逻辑)，后缀词(值、距离、逻辑)}，中心距离只能为空，前后缀值/距离/逻辑应该同时存在或不存在。
    # 测试文本：[(字符串,Y/N),(字符串,Y/N),(字符串,Y/N)]，对同一个字典做一系列测试，输出匹配错误的case
    regexp_para = dict(
        center=dict(
            value="中心",
            distance="",
            logic="包含",
        ),
        head=dict(
            value="前缀|前面",
            distance="3",
            logic="不包含",
        ),
        tail=dict(
            value="后缀|后面",
            distance="2",
            logic="不包含",
        ),
    )
    test_case = [
        (u"中心", True),
        (u"中心后缀", False),
        (u"前面是是中心的后缀", False),
        (u"前缀是是是中心不不后缀", True),
        (u"前缀是是是中心不后缀的", False),
        (u"前缀是是中心不不后面", False)
    ]
    unit_test(regexp_para, test_case)
