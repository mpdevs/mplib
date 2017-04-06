# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from mplib.IO import PostgreSQL


def get_attr_pnv():
    db = PostgreSQL()
    sql = """
    SELECT
      attr_name, attr_value
    FROM attr_value
    WHERE industry_id = 16
     AND attr_name <> '品牌'
    """
    ret = db.query(sql)
    return ret


def get_word_vec():
    pass


def process_wv():
    with open("vectors.txt") as fw:
        for i in range(3):
            print(fw.readline())


if __name__ == "__main__":
    # print(get_attr_pnv())
    process_wv()
