# coding: utf-8
# __author__: u"John"
"""
用于原生的list或者dict类型的内存对象数据导出和导入
如果是继承list或者dict类, 则可能无法正常导出
通常用于文本文件、数据库返回结果
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import with_statement
from __future__ import print_function
from __future__ import division
from os.path import splitext, basename, dirname, join
from io import open
import pickle


def pickle_dump(file_name, dump_object, ext="pickle"):
    """
    导出pickle文件时自动添加.pickle后缀
    :param file_name:
    :param dump_object:
    :param ext:
    :return:
    """
    dir_name = dirname(file_name)
    file_name, ext_name = splitext(basename(file_name))
    pickle_name = "{0}.{1}".format(file_name, ext)

    with open(join(dir_name, pickle_name), "wb") as f:
        pickle.dump(dump_object, f)


def pickle_load(file_name, ext="pickle"):
    """
    读文件的时候可以忽略 .pickle 的后缀
    :param file_name:
    :param ext:
    :return:
    """
    dir_name = dirname(file_name)
    file_name, ext_name = splitext(basename(file_name))
    pickle_name = "{0}.{1}".format(file_name, ext)

    with open(join(dir_name, pickle_name), "rb") as f:
        load_object = pickle.load(f)

    return load_object



