# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.dnr.factory import DataDenoiser
from mplib.common import smart_decode
import traceback
import pandas
import sys
import csv

reload(sys)
sys.setdefaultencoding("utf8")

try:
    # reader = pandas.read_table(sys.stdin, header=None, sep='\t', quoting=csv.QUOTE_NONE, encoding='utf-8', index_col=None, iterator=True)
    # while True:
    #     try:
    #         chunk = reader.get_chunk(2000)
    #         dd = DataDenoiser(chunk)
    #         dd.use_keywords = True
    #         dd.use_tag = False
    #         dd.use_length = True
    #         dd.use_series = False
    #         dd.use_client = False
    #         dd.use_special_characters = False
    #         dd.use_edit_distance = False
    #         dd.run()
    #     except StopIteration:
    #         break
    data = [line for line in sys.stdin]
    data = map(lambda x: smart_decode(x.replace(b"\n", b"").replace(b"\r", b"")).split("\t"), data)
    dd = DataDenoiser(data)
    dd.use_keywords = True
    dd.use_tag = False
    dd.use_length = True
    dd.use_series = False
    dd.use_client = False
    dd.use_special_characters = False
    dd.use_edit_distance = False
    dd.run()
except Exception as e:
    print "\t".join([str(e), traceback.format_exc()])

