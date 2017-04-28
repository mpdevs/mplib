# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.competition import GoldMiner
from mplib.common import smart_decode
import traceback
import sys


if __name__ == "__main__":

    reload(sys)
    sys.setdefaultencoding("utf8")

    try:
        data = [line for line in sys.stdin]
        data = list(map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data))
        tagging = GoldMiner()
        gm = GoldMiner()
        gm.category_id = sys.argv[1]
        gm.data = data
        gm.mold()
        gm.smelt()

    except:
        pass
