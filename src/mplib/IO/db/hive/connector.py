# coding: utf-8
# __author__ = "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.setting import HIVE_CONNECTION, IDC_HIVE_CONNECTION
from mplib.common import smart_decode, smart_encode
import pyhs2


def get_env(env="local"):
    return get_env_dict().get(env, HIVE_CONNECTION)


def get_env_dict():
    return dict(
        local=HIVE_CONNECTION,
        idc=IDC_HIVE_CONNECTION,
    )


class Hive:
    def __init__(self, env="local"):
        self.conn = pyhs2.connect(**get_env(env))
        self.cursor = None

    def __del__(self):
        self.close()

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
        self.cursor = self.conn.cursor()
        self.cursor.execute(smart_encode(sql))
        columns = [smart_decode(row["columnName"]) for row in self.cursor.getSchema()]
        if to_dict:
            rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in self.cursor]
        else:
            rows = smart_decode([row for row in self.cursor])

        if meta:
            return rows, columns
        else:
            return rows

    def total(self, sql):
        self.cursor = self.conn.cursor()
        self.cursor.execute(smart_encode(sql))
        columns = [smart_decode(row["columnName"]) for row in self.cursor.getSchema()]
        rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in self.cursor]
        if rows:
            return rows[0][columns[0]]
        else:
            return 0

    def fetch_raw(self, sql):
        self.cursor = self.conn.cursor()
        self.cursor.execute(smart_encode(sql))
        rows = []
        for row in self.cursor:
            rows.append("\t".join([smart_decode(_, cast=True) if _ else "" for _ in row]))
        return rows

    def execute(self, sql):
        self.cursor = self.conn.cursor()
        for s in sql.split(";"):
            s = s.strip()
            if s:
                self.cursor.execute(smart_encode(s))

    def close(self):
        self.conn.close()

    @staticmethod
    def get_env_dict():
        return get_env_dict()

    @staticmethod
    def show_env_dict():
        from pprint import pprint
        pprint(get_env_dict())


if __name__ == "__main__":
    Hive.show_env_dict()
    data = []
    hive = Hive("local")
    template = "SELECT * FROM kol.kol LIMIT 20"
    for i in range(10):
        data.append(hive.query(template))
    for _ in data:
        print(len(_))
