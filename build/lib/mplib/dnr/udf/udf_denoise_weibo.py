# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from mplib.dnr.factory import DataDenoiser
from mplib.common import smart_decode
import traceback
import sys

reload(sys)
sys.setdefaultencoding("utf8")


keywords = [
    "分享",
    "【.*?】",
    "新歌",
    "红包",
    "小伙伴",
    "围观",
    "领取",
    "加入",
]


def swap(lst):
    return [lst[1], lst[0]]


try:
    data = [line for line in sys.stdin]
    data = map(lambda x: swap(smart_decode(x).replace("\n", "").replace("\r", "").replace("\\N", "").split("\t")), data)
    dd = DataDenoiser(data=data, content_index=1, head=["id", "content"])
    dd.use_keywords = True
    dd.noise_keywords_list = keywords
    dd.use_length = True
    dd.noise_length_min = 5
    dd.noise_length_max = 500
    dd.use_tag = True
    dd.noise_tag_brackets = "★|◆"
    dd.udf_support = True
    dd.run()
except Exception as e:
    print("\t".join(["ERROR", traceback.format_exc().replace("\t", " ").replace("\n", " ")]))
