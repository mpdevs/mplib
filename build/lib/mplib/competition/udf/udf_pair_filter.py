# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.competition import GoldMiner
from mplib.common import smart_decode
import traceback
import sys


reload(sys)
sys.setdefaultencoding("utf8")

try:
    data = [line for line in sys.stdin]
    data = list(map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data))
    gm = GoldMiner()
    gm.category_id = "1623"
    gm.data = data
    gm.pan()
    gm.refine()
    gm.smelt()

except Exception as e:
    print("\t".join([traceback.format_exc(), "ERROR", "ERROR", "ERROR"]))
