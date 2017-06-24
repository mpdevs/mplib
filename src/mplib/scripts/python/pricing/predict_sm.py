# coding: utf8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.pricing.helper import split_id_feature, split_x_y
from mplib.pricing import SKAssess
from mplib.common import smart_decode
from mplib.IO import Hive
from mplib import *
import traceback
import sys


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf8")

    try:
        a = SKAssess()
        a.category_id = sys.argv[1]
        a.interval = smart_decode(sys.argv[2])
        sql = "SELECT itemid AS itemid, data AS data FROM elengjing_price.tmp_{0} LIMIT 10".format(a.category_id)
        data = Hive("idc").query(sql)
        data = ["\t".join([str(line.get("itemid")), str(line.get("data"))]) for line in data]
        data = list(map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data))
        items, data = split_id_feature(data)
        a.x_predict, a.y_predict = split_x_y(data)
        print(a.y_predict.shape)
        print(a.x_predict.shape)
        a.path = "/Users/panjunjun/PycharmProjects/Lab/IO/sklearn_randomforest_{0}_daily.pickle".format(a.category_id)
        a.load_model()
        a.predict()
        data = zip(items, a.prediction.astype(str).tolist())
        for line in data:
            # print("\t".join([line[0], ",".join(line[1])]))
            pass

    except Exception as e:
        print("\t".join(["ERROR", traceback.format_exc()]))
