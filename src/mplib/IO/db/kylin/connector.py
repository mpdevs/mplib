# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.setting import KYLIN_CONNECTION
import pykylin


def get_env(env):
    return get_env_dict().get(env, KYLIN_CONNECTION)


def get_env_dict():
    return dict(
        local=KYLIN_CONNECTION
    )


class Kylin:
    def __init__(self, env="local"):
        self.conn = pykylin.connect(**get_env(env))

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
    WITH a AS (
        SELECT
            c.categoryid,
            c.categoryname,
            COUNT(DISTINCT itemid) AS spu
        FROM women_clothing_item AS i
        JOIN category AS c
        ON i.categoryid = c.categoryid
        GROUP BY
            c.categoryid,
            c.categoryname
    )
    SELECT * FROM a
    """
    pprint(Kylin().query(test_sql))

    pprint(Kylin.get_env_dict())
