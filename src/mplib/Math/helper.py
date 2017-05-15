# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from math import ceil


def get_chunk_number(total_size, chunk_size=100, truncate=False):
    number = float(total_size) / float(chunk_size)
    return int(number) if truncate else int(ceil(number))


if __name__ == "__main__":
    print(get_chunk_number(1))
    print(get_chunk_number(1, truncate=True))
