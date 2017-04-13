# coding: utf-8
# __author__: u"John"
"""
This module contains debug / print tools
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

from collections import OrderedDict
from datetime import datetime
from six import iteritems

from .unicode_tool import to_unicode
from .setting import DEBUG


def print_line(placeholder="-", repeat=50, center_word=""):
    """
    打印一行分隔符
    :param placeholder: 分割符基本元素
    :param repeat: 分隔符基本元素分别在前面和后面出现的次数, 必须是int
    :param center_word: 中心显示的内容
    :return:
    """
    if not DEBUG:
        return

    if isinstance(repeat, long) or isinstance(repeat, int):
        pass
    elif isinstance(repeat, float):
        repeat = int(repeat)
    elif isinstance(repeat, basestring):
        try:
            repeat = int(repeat)
        except ValueError:
            return
    else:
        return

    repeat = repeat if repeat <= 80 else 80
    center_word = " {0} ".format(to_unicode(center_word)) if center_word else center_word
    print("{0}{1}{0}".format(to_unicode(placeholder) * repeat, center_word))


def get_print_var(var_dict, var_list):
    od = OrderedDict()

    for i in var_list:
        od[i] = None

    for k, v in iteritems(var_dict):
        if k in list(od):
            od[k] = v

    return od


def print_var_info(var_dict, var_list):
    for k, v in iteritems(get_print_var(var_dict, var_list)):
        d("{0}: {1}".format(k, v))


def d(string):
    print("{0} {1}".format(datetime.now(), string))


if __name__ == "__main__":
    print_line()
    print_line("a")
    print_line("b", "30")
    print_line("b", "30", "hello")
    print_line("b", "a", "hello")
    print_line("_", 100, "你好")
    print_line(["_", "-"], 1, dict(hello="world"))
