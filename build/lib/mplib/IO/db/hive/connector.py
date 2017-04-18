# coding: utf-8
# __author__ = "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.common.setting import HIVE_CONNECTION, IDC_HIVE_CONNECTION
from mplib.common import smart_decode, smart_encode
import pyhs2


def get_env(env="local"):
    env_dict = dict(
        local=HIVE_CONNECTION,
        idc=IDC_HIVE_CONNECTION,
    )
    return env_dict.get(env, HIVE_CONNECTION)


class Hive:
    def __init__(self, pool_name="poolHigh", env="local"):
        # 设置超时时间30秒无响应即关闭连接
        self.conn = pyhs2.connect(**get_env(env))
        self.pool_name = pool_name

    def get(self, sql):
        rows = self.query(sql.encode("utf8"))
        return rows[0] if len(rows) > 0 else None

    def query(self, sql, meta=False):
        """
        :param sql:
        :param meta: True的时候同时返回表头信息
        :return:
        """
        with self.conn.cursor() as cursor:
            # 设置pool
            cursor.execute("set mapred.fairscheduler.pool={0}".format(self.pool_name))
            cursor.execute(smart_encode(sql))
            columns = [smart_decode(row["columnName"]) for row in cursor.getSchema()]
            rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in cursor]
            self.close()

            if meta:
                return rows, columns
            else:
                return rows

    def total(self, sql):
        with self.conn.cursor() as cursor:
            # 设置pool
            cursor.execute("set mapred.fairscheduler.pool={0}".format(self.pool_name))
            cursor.execute(smart_encode(sql))
            columns = [smart_decode(row["columnName"]) for row in cursor.getSchema()]
            rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in cursor]
            self.close()
            if rows:
                return rows[0][columns[0]]
            else:
                return 0

    def execute(self, sql):
        with self.conn.cursor() as cursor:
            # 设置pool
            cursor.execute("set mapred.fairscheduler.pool={0}".format(self.pool_name))
            for s in sql.split(";"):
                if s.strip():
                    cursor.execute(smart_encode(s))
            self.close()

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    from pprint import pprint
    Hive(env="idc").execute("CREATE TABLE tmp(id string); DROP TABLE tmp;")
    pprint(Hive(env="idc").query("SHOW TABLES"))
