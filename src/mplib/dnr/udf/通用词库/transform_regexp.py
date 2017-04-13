# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

from mplib.common.helper import print_line
from io import open


def transformer(regexp_string):
    regexp_string = regexp_string.replace("\r", "").replace("\n", "")
    regexp_string = regexp_string.replace("\d", r"\\d")
    regexp_string = regexp_string.replace("\D", r"\\D")
    return regexp_string


print_line(center_word="母婴")
with open("通用去水--母婴论坛贴吧.txt", "r", encoding="utf8") as f:
    data = list(map(transformer, [i for i in f]))
    for line in data:
        print("'{0}',".format(line))

print_line(center_word="微博")
with open("通用去水-微博.txt", "r", encoding="utf8") as f:
    data = list(map(transformer, [i for i in f]))
    for line in data:
        print("'{0}',".format(line))

print_line(center_word="问诊")
with open("通用去水-问诊.txt", "r", encoding="utf8") as f:
    data = list(map(transformer, [i for i in f]))
    for line in data:
        print("'{0}',".format(line))
