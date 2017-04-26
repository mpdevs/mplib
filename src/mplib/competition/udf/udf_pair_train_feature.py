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


if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding("utf8")

    try:
        tagging = GoldMiner()
        data = [line for line in sys.stdin]
        data = list(map(lambda x: smart_decode(x).replace("\n", "").replace("\r", "").split("\t"), data))
        gm = GoldMiner()
        gm.category_id = sys.argv[1]
        gm.data = data
        gm.refine()
        gm.smelt()

    except Exception as e:
        print("\t".join(["ERROR"] * 3 + [traceback.format_exc()]))
