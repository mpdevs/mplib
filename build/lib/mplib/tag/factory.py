# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

from mplib.common.settings import MYSQL_SETTINGS
from mplib.common import AttributeDict
from mplib.IO import MySQL


class Parser(object):
    """
    解析Mysql的标签
    """
    def __init__(self):
        self.code_message = AttributeDict(code=0, message=u"")
        self.column_separator = u","
        self.value_separator = u":"
        self.parse_index = None
        self.sql = None
        self.raw_list = []
        self.parsed_list = []
        self.current_tag = AttributeDict()
        return

    def get_content(self):
        db = MySQL(**MYSQL_SETTINGS)
        self.raw_list = db.query(self.sql)
        return

    def parse(self):
        for idx, line in enumerate(self.raw_list):
            self.current_tag = AttributeDict()
            tmp_line = line[self.parse_index][1:-1].split(self.column_separator)
            for pair in tmp_line:
                try:
                    dimension, value = tuple(pair.split(self.value_separator))
                    if dimension in list(self.current_tag):
                        self.current_tag[dimension] += [value]
                    else:
                        self.current_tag[dimension] = [value]
                except TypeError:
                    print(tmp_line)
            self.parsed_list.append(self.current_tag)
        return

    def main(self, sql, index):
        self.parse_index = index
        self.sql = sql
        self.get_content()
        self.parse()
        return

