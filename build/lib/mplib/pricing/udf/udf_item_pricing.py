# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.pricing.helper import split_id_feature, split_x_y
from mplib.pricing import SKAssess
from mplib.common import smart_decode
import traceback
import sys

reload(sys)
sys.setdefaultencoding("utf8")

try:
    data = [line for line in sys.stdin]
    data = list(map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data))
    items, arrays = split_id_feature(data)
    a = SKAssess()
    a.x_train, a.y_train = split_x_y(arrays)
    a.predict()
except Exception as e:
    print("\t".join(["ERROR", traceback.format_exc()]))
