# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.pricing.helper import get_all_category
from mplib.pricing.api import hive_data_predict


if __name__ == "__main__":
    for i in get_all_category():
        hive_data_predict(i)

