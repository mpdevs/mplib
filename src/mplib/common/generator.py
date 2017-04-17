# coding: utf-8
# __author__: u"John"
"""
This module contains global genderators
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
import string


def g_fib(index):
    """
    斐波那契数列
    :param index: 数列大小
    :return:
    """
    n, a, b = 0, 0, 1
    while n < index:
        yield b
        a, b = b, a + b
        n += 1


def excel_col_name(index, lowercase=False):
    """
    生成类似Excel的列名
    例如 A-Z, AA-ZZ, AAA-ZZZ
    :param index: 列的数量
    :param lowercase:
    :return:
    """
    return index >= 0 and excel_col_name(index // 26 - 1) + chr(97 if lowercase else 65 + index % 26) or ""


def g_excel_col_name(index, lowercase=False):
    """
    生成类似Excel的列名的生成器
    :param index:
    :param lowercase:
    :return:
    """
    n = 0
    while n < index:
        yield excel_col_name(n, lowercase)
        n += 1


def g_alphabet(index, lowercase=False):
    """
    生成A-Z或a-z的序列
    :return:
    """
    n = 0
    while n < index:
        _, m = divmod(n, 26)
        yield string.ascii_lowercase[m] if lowercase else string.ascii_uppercase[m]
        n += 1
    return


if __name__ == "__main__":
    from mplib.common.helper import print_line
    print_line(center_word="g_fib")
    print([i for i in g_fib(6)])
    print_line(center_word="g_alphabet")
    print([i for i in g_alphabet(100, True)])
    print([i for i in g_alphabet(100, False)])
    print_line(center_word="g_excel_col_name")
    print([i for i in g_excel_col_name(27)])
