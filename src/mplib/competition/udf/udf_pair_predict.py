# coding: utf8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.competition.helper import split_x_y
from mplib.competition import Hound
from mplib.common import smart_decode
from mplib import *
import traceback
import sys


if __name__ == "__main__":

    reload(sys)
    sys.setdefaultencoding("utf8")

    try:
        data = [line for line in sys.stdin]
        data = list(map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data))

        h = Hound()
        h.x_predict, h.y_predict = split_x_y(data)
        h.category_id = sys.argv[1]
        h.load_model()
        h.predict()
        data = list(map(lambda x, y: x + y, [line[:4] for line in data], h.prediction.astype(unicode).tolist()))

        for line in data:
            print("\t".join(line))

    except Exception as e:
        print("\t".join(["ERROR"] * 4 + [traceback.format_exc()]))
