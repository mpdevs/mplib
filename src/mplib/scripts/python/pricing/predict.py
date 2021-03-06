# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.pricing.helper import get_all_category
from mplib.pricing.api import hive_data_predict
from datetime import datetime
import sys


if __name__ == "__main__":
    category_list = get_all_category()
    try:
        is_event = sys.argv[1]
        is_event = is_event == "event"
    except:
        is_event = True
    # print("{0} 获取品类成功".format(datetime.now()))
    for idx, category_id in enumerate(category_list):
        # print("{0} 正在处理品类 {1} ({2}/{3})".format(datetime.now(), category_id, idx + 1, len(category_list)))
        hive_data_predict(category_id, is_event, True)
