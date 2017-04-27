# coding: utf-8
# __author__ = "John"
from __future__ import unicode_literals, absolute_import, print_function, division
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
    def __init__(self, env="local", pool_name="poolHigh"):
        # 设置超时时间30秒无响应即关闭连接
        self.conn = pyhs2.connect(**get_env(env))
        self.cursor = self.conn.cursor()
        if env == "idc":
            self.pool_name = pool_name
            self.cursor.execute("set mapred.fairscheduler.pool={0}".format(self.pool_name))
            self.cursor.execute("set ngmr.partition.automerge=true")
            self.cursor.execute("set ngmr.partition.mergesize.mb=200")

    def get(self, sql):
        rows = self.query(smart_encode(sql))
        return rows[0] if len(rows) > 0 else None

    def query(self, sql, meta=False, to_dict=True):
        """
        :param sql:
        :param meta: True的时候同时返回表头信息
        :param to_dict: True将返回字典类型, False返回列表类型
        :return:
        """
        self.cursor.execute(smart_encode(sql))
        columns = [smart_decode(row["columnName"]) for row in self.cursor.getSchema()]
        if to_dict:
            rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in self.cursor]
        else:
            rows = smart_decode([row for row in self.cursor])
        self.close()

        if meta:
            return rows, columns
        else:
            return rows

    def total(self, sql):
        self.cursor.execute(smart_encode(sql))
        columns = [smart_decode(row["columnName"]) for row in self.cursor.getSchema()]
        rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in self.cursor]
        self.close()
        if rows:
            return rows[0][columns[0]]
        else:
            return 0

    def execute(self, sql):
        for s in sql.split(";"):
            if s.strip():
                self.cursor.execute(smart_encode(s))
        self.close()

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    from pprint import pprint
    Hive(env="idc").execute("CREATE TABLE alahubake(id string); DROP TABLE alahubake;")
    pprint(Hive(env="idc").query("SHOW TABLES"))
    pprint(Hive(env="idc").query("SELECT dateid, date FROM dimdate LIMIT 1", to_dict=False))
