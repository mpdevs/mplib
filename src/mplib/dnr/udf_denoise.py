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
    dd.use_tag = False
    dd.use_length = True
    dd.use_series = False
    dd.use_client = False
    dd.use_special_characters = False
    dd.use_edit_distance = False
    dd.udf_support = True
    dd.run()
except Exception as e:
    print "\t".join([str(e), traceback.format_exc()])

