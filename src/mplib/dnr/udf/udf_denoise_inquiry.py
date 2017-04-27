# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.dnr.factory import DataDenoiser
from mplib.common import smart_decode
import traceback
import sys

reload(sys)
sys.setdefaultencoding("utf8")


keywords = [
    '通知分享',
    '通知通知',
    '通知出停诊',
    '停诊通知',
    '填表',
    '预约转诊服务',
]


def swap(lst):
    return [lst[1], lst[0]]


try:
    data = [line for line in sys.stdin]
    data = list(map(lambda x: swap(smart_decode(x).replace("\n", "").replace("\r", "").replace("\\N", "").split("\t")), data))
    dd = DataDenoiser(data=data, content_index=1, head=["id", "content"])
    dd.use_keywords = True
    dd.noise_keywords_list = keywords
    dd.udf_support = True
    dd.run()
except Exception as e:
    print("\t".join(["ERROR", traceback.format_exc().replace("\t", " ").replace("\n", " ")]))
