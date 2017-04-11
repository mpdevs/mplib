# coding: utf-8
# __author__: u"Zhoujianfeng"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from .reg_abc import reg_abc
import re


def unit_test(regexp_string, test_case):
    # 正则实现
    print(regexp_string)
    for line in test_case:
        sample_text, r = line
        # print sample_text
        try:
            if re.search(regexp_string, sample_text) is None:
                rr = False
            else:
                rr = True
            if rr != r:
                print(sample_text, r)
        except Exception as e:
            # print e
            print("reg_abc怎么还有bug？")


if __name__ == "__main__":
    # 字典结构：{中心词(值、距离、逻辑)，前缀词(值、距离、逻辑)，后缀词(值、距离、逻辑)}，中心距离只能为空，前后缀值/距离/逻辑应该同时存在或不存在。
    # 测试文本：[(字符串,Y/N),(字符串,Y/N),(字符串,Y/N)]，对同一个字典做一系列测试，输出匹配错误的case
    regexp_para = dict(
        center=dict(
            value="哪|推荐|介绍|怎么",
            distance="",
            logic="包含",
        ),
        head=dict(
            value="无不没都",
            distance="5",
            logic="不包含",
        ),
        tail=dict(
            value="无|不|没|都|还",
            distance="5",
            logic="不包含",
        ),
    )
    test_case = [
        ("一般情况下都是主张母乳喂养的哦，如果母乳不够真要喝奶粉也(1,3,4,5)没有说哪个好不(2)好，只能说适不适合宝宝，我家是喝美素(6)的", False),
    ]
    unit_test(reg_abc(regexp_para), test_case)
    print("-" * 100)
    unit_test("(消化)([^(不)]){,1}(非常|好)", test_case)
    # a = "(消化)([^(不)]{5,}|[^(不)]*$)([^(非常|好)]{,4})(非常|好)"
    # b = "(消化)([^(非常|好)]{,4})(非常|好)"
