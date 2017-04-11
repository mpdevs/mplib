# coding: utf-8
# __author__: u"John"
"""
用于原生的list或者dict类型的内存对象数据导出和导入
如果是继承list或者dict类, 则可能无法正常导出
通常用于文本文件、数据库返回结果
"""
from __future__ import unicode_literals
from __future__ import with_statement
from __future__ import print_function
from __future__ import division
from os.path import splitext
from io import open
import pickle


def pickle_dump(file_name, dump_object):
    """
    导出pickle文件时自动添加.pickle后缀
    :param file_name:
    :param dump_object:
    :return:
    """
    if splitext(__file__)[-1][1:] == "pickle":
        pickle_name = file_name
    else:
        pickle_name = file_name + ".pickle"

    with open(pickle_name, "wb") as f:
        pickle.dump(dump_object, f)

    return


def pickle_load(file_name):
    """
    读文件的时候可以忽略 .pickle 的后缀
    :param file_name:
    :return:
    """
    if splitext(__file__)[-1][1:] == "pickle":
        pickle_name = file_name
    else:
        pickle_name = file_name + ".pickle"

    with open(pickle_name, "rb") as f:
        load_object = pickle.load(f)

    return load_object



