# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from math import factorial


def npr(n, r):
    try:
        n = int(n)
        r = int(r)
        return factorial(n) / factorial(n-r)
    except ValueError:
        return -1


def ncr(n, r):
    try:
        n = int(n)
        r = int(r)
        return npr(n, r) / factorial(r)
    except ValueError:
        return -1


if __name__ == "__main__":
    print(ncr(6, 4))
    print(ncr(10, 4))
