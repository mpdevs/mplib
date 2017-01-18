# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
import string


def fib(index):
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


def excel_col_name(index, c=""):
    """
    生成类似Excel的列名
    例如 A-Z, AA-ZZ, AAA-ZZZ
    :param index: 列的数量
    :param c:
    :return:
    """
    print_line()
    if index > 0:
        index, m = divmod(index, 26)
        print index, m
        c = (string.ascii_uppercase[index - 1] if index else string.ascii_uppercase[m - 1]) + c
        excel_col_name(index, c)
    return index, c


f=lambda i:i>=0and f(i/26-1)+chr(65+i%26)or''
def f1(i):
    return i >= 0 and f1(i / 26 - 1) + chr(65 + i % 26) or ''

if __name__ == "__main__":
    from helpers import print_line
    # print_line(center_word=fib.__name__)
    # fibonacci = fib(6)
    # print type(fibonacci)
    # for i in fibonacci:
    #     print i
    # print_line()
    # excel_col_name(1)
    # print_line()
    # print reduce(lambda x, y: x + y, fib(6))
    # print_line()
    print excel_col_name(26)
