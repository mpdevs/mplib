# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.competition import GoldMiner, Hound
from mplib.IO import Hive, pickle_dump, pickle_load
from mplib.common import smart_decode
from six import iteritems


def gold_miner_local_test():
    gm = GoldMiner()
    print(list(gm.essential_dict))
    gm.category_id = "1623"
    gm.data = [
        [
            "颜色:黑色,图案:纯色,工艺:绣花,尺码:S,尺码:XXL,尺码:XS,适用年龄:25-29周岁,尺码:M,裙长:长裙,面料:其他,尺码:L,上市季节:2017年春季,尺码:XL",
            "尺码:XS,适用年龄:25-29周岁,尺码:L,上市季节:2015年冬季,腰型:高腰,裙长:短裙",
            "544570113012",
            "539017859116",
        ]
    ]
    gm.pan()
    gm.smelt()


def get_minerals_from_hive():
    pickle_dump("raw_data", Hive(env="idc").query(sql="SELECT customer_attr, target_attr, customer_item_id, target_item_id FROM competitive_filtering_stage_1 LIMIT 10000;", to_dict=False))


def gold_miner_hive_test():
    gm = GoldMiner()
    gm.data = smart_decode(pickle_load("raw_data"), cast=True)
    gm.pan()
    print(len(gm.data))
    gm.smelt()


def get_train_data_from_hive():
    pickle_dump("cleaned_data", Hive(env="idc").query(sql="", to_dict=False))


def train_hound():
    h = Hound()
    h.data = smart_decode(pickle_load("raw_data"), cast=True)

if __name__ == "__main__":
    # gold_miner_unit_test()
    # get_minerals_from_hive()
    gold_miner_hive_test()
