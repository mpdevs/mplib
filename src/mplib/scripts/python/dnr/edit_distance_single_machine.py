# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.IO import Hive, pickle_dump, pickle_load
from mplib.common import smart_encode
import editdistance


def get_data():
    sql = """
    SELECT
        d.id,
        d.content
    FROM das.ci_diaper2014to2016_0406_allplatform_distinct AS d
    WHERE d.year = '2016'
    """
    data = Hive(env="local").query(sql, to_dict=False)
    pickle_dump("raw", data)


def show_tables():
    print(Hive().query("show tables"))


def transform():
    pickle_dump("id_content", {line[0]: line[1] for line in pickle_load("raw")})


if __name__ == "__main__":
    get_data()
