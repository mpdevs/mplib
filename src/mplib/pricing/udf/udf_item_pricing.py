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
    items, data = split_id_feature(data)
    a = SKAssess()
    a.x_predict, a.y_predict = split_x_y(data)
    a.load_model()
    a.predict()
    data = zip(items, a.prediction.astype(str).tolist())
    for line in data:
        print("\t".join([line[0], ",".join(line[1])]))

except Exception as e:
    print("\t".join(["ERROR", traceback.format_exc()]))
