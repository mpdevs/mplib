# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from .factory import Parser


def parse_tag(sql, index=0):
    p = Parser()
    p.main(sql=sql, index=index)
    return p.parsed_list


if __name__ == "__main__":
    test_sql = "SELECT TaggedItemAttr FROM mp_women_clothing.TaggedItemAttr LIMIT 10;"
    print(parse_tag(test_sql))
