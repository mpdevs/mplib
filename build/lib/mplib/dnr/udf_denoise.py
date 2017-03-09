# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.dnr.api import das_denoise
from mplib.common import smart_decode, smart_encode
from mplib.IO import MySQL
import traceback
import json
import sys

reload(sys)
sys.setdefaultencoding("utf8")

# try:
#     data = [line for line in sys.stdin]
#     data = map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data)
#     dd = DataDenoiser(data=data, content_index=1, head=["id", "content"])
#     dd.use_keywords = True
#     dd.use_tag = False
#     dd.use_length = True
#     dd.use_series = False
#     dd.use_client = False
#     dd.use_special_characters = False
#     dd.use_edit_distance = False
#     dd.udf_support = True
#     dd.run()
# except Exception as e:
#     print "\t".join([str(e), traceback.format_exc()])
try:
    # data = [line for line in sys.stdin]
    # data = map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data)
    ret = MySQL().query("SELECT json_value FROM das.process_parameters", cast=True)
    print json.loads(ret[0].get("json_value")).get("noise_client_list").split(",")
    for i in json.loads(ret[0].get("json_value")).get("noise_client_list").split(","):
        print i
except Exception as e:
    print "\t".join([str(e), traceback.format_exc()])
