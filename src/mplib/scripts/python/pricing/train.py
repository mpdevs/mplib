# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.pricing.api import hive_data_train
from mplib.pricing.helper import get_all_category
from datetime import datetime
import sys


if __name__ == "__main__":
    category_list = get_all_category()
    try:
        is_event = sys.argv[1]
        is_event = is_event == "event"
    except:
        is_event = False
    print("{0} 获取品类成功".format(datetime.now()))
    # for idx, category_id in enumerate(category_list):
    import os
    for idx, category_id in enumerate([50008897]):
        print("{0} 正在处理品类 {1} ({2}/{3})".format(datetime.now(), category_id, idx + 1, len(category_list)))
        # hive_data_train(category_id, is_event, path="/home/script/normal_servers/serverudf/elengjing/data/pricing_model")
        hive_data_train(category_id, is_event, path=os.getcwd())
