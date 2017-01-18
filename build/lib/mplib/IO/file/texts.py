# coding: utf-8
# __author__: u"John"
from __future__ import with_statement
import pandas


def to_df(file_path, sep="\t", encoding="utf8"):
    """
    传入一个文本文件的绝对路径, 返回pandas的DataFrame对象, 可选编码和分隔符
    :param file_path:
    :param sep:
    :param encoding:
    :return:
    """
    data = []
    with open(name=file_path, mode="r", encoding=encoding) as file_object:
        columns = file_object.readline().strip().split(sep=sep)
        for line in file_object:
            line = line.strip().split(sep=sep)
            if len(columns) != len(line):
                continue
            data.append(line)
    return pandas.DataFrame(data=data, columns=columns)
