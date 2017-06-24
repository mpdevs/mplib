# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.setting import KYLIN_CONNECTION, IDC_KYLIN_CONNECTION
import pykylin


def get_env(env):
    return get_env_dict().get(env, KYLIN_CONNECTION)


def get_env_dict():
    return dict(
        local=KYLIN_CONNECTION,
        idc=IDC_KYLIN_CONNECTION,
    )


class Kylin(object):
    def __init__(self, env="local", limit=50000):
        self.conn = pykylin.connect(limit=limit, **get_env(env))

    def get(self, sql):
        rows = self.query(sql)
        return rows[0] if len(rows) > 0 else None

    def query(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            columns = [column[0].lower() for column in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.results]
            return rows
        except Exception as e:
            raise e

    def total(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            return cursor.results[0][0]
        except Exception as e:
            raise e

    @staticmethod
    def get_env_dict():
        return get_env_dict()

    @staticmethod
    def show_env_dict():
        from pprint import pprint
        pprint(get_env_dict())


if __name__ == "__main__":
    from pprint import pprint
    test_sql = """
    SELECT
        i.shopid,
        COUNT(DISTINCT i.itemid) AS kpi
    FROM elengjing.women_clothing_item AS i

    WHERE i.daterange BETWEEN '2016-12-11' AND '2016-12-17'
    AND i.platformid = '7011'

    GROUP BY
        i.shopid
    """
    pprint(Kylin("idc").query(test_sql))
    # pprint(Kylin.get_env_dict())
