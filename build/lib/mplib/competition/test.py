# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.competition import GoldMiner, Hound
from mplib.competition.helper import split_x_y
from mplib.IO import Hive, pickle_dump, pickle_load
from mplib.common import smart_decode, time_elapse
from mplib import *
import time


@time_elapse
def gold_miner_local_test():
    gm = GoldMiner()
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


@time_elapse
def get_minerals_from_hive():
    sql = """
    SELECT
        customer_attr,
        target_attr,
        customer_item_id,
        target_item_id
    FROM t_elengjing.competitive_filtering_stage_1
    LIMIT 10000;
    """
    pickle_dump("raw_data", Hive(env="idc").query(sql=sql, to_dict=False))


@time_elapse
def gold_miner_hive_test():
    gm = GoldMiner()
    gm.data = smart_decode(pickle_load("raw_data"), cast=True)
    gm.pan()
    print(len(gm.data))
    gm.smelt()


@time_elapse
def get_train_data_from_hive():
    sql = "SELECT * FROM t_elengjing.competitive_item_train_stage_2"
    pickle_dump("cleaned_data", Hive(env="idc").query(sql=sql, to_dict=False))


@time_elapse
def train_hound():
    h = Hound()
    h.x_train, h.y_train = split_x_y(smart_decode(pickle_load("train_data"), cast=True))
    h.train()
    h.save_model()
    h.print_info()


@time_elapse
def cast_hound():
    h = Hound()
    data = smart_decode(pickle_load("train_data"), cast=True)[:10]
    h.x_predict, h.y_predict = split_x_y(data)
    h.load_model()
    h.predict()
    h.print_info()
    data = list(map(lambda x, y: x + y, [line[:4] for line in data], h.prediction.astype(unicode).tolist()))
    for line in data:
        print(len("\t".join(line)))


@time_elapse
def gold_miner_mold():
    gm = GoldMiner()
    gm.data = pickle_load("cleaned_data")[:20]
    print(len(gm.data))
    print(len(gm.data[0]))
    start = time.time()
    gm.mold()
    print(len(gm.data[0]))
    print("elapse {0} seconds".format(time.time() - start))


def get_gold_from_hive():
    sql = "SELECT * FROM t_elengjing.competitive_item_train_stage_3"
    pickle_dump("train_data", Hive(env="idc").query(sql, to_dict=False))


if __name__ == "__main__":
    # gold_miner_unit_test()
    # get_minerals_from_hive()
    # gold_miner_hive_test()
    # gold_miner_mold()
    # get_train_data_from_hive()
    # gold_miner_mold()
    # get_gold_from_hive()
    # train_hound()
    cast_hound()
