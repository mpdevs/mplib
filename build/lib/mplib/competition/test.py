# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.competition import GoldMiner


def gold_miner_unit_test():
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


gold_miner_unit_test()
