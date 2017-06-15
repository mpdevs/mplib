# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.pricing.helper import get_all_category
from mplib.pricing.api import hive_data_predict
import sys


if __name__ == "__main__":
    try:
        is_event = sys.argv[1]
        is_event = is_event == "event"
    except:
        is_event = False
    for category_id in get_all_category():
        hive_data_predict(category_id, is_event)
