# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.dnr.factory import DataDenoiser
from mplib.common import smart_decode
import traceback
import sys

reload(sys)
sys.setdefaultencoding("utf8")

try:
    data = [line for line in sys.stdin]
    data = map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data)
    dd = DataDenoiser(data=data, content_index=1, head=["id", "content"])
    dd.use_keywords = True
    dd.use_length = True
    dd.noise_length_min = 5
    dd.noise_length_max = 500
    dd.udf_support = True
    dd.run()
except Exception as e:
    print "\t".join(["ERROR", traceback.format_exc()])
