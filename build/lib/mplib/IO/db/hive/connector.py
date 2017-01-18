# coding: utf-8
# __author__ = "John"
from __future__ import unicode_literals
from mplib.common.settings import HIVE_CONNECTION
from mplib.common import smart_decode
import pyhs2


class Hive:
    def __init__(self, pool_name="poolHigh"):
        # 设置超时时间30秒无响应即关闭连接
        self.conn = pyhs2.connect(**HIVE_CONNECTION)
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
            cursor.execute(sql.encode("utf8"))
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
            cursor.execute(sql.encode("utf8"))
            columns = [smart_decode(row["columnName"]) for row in cursor.getSchema()]
            rows = [dict(zip(columns, [smart_decode(cell) for cell in row])) for row in cursor]
            self.close()
            if rows:
                return rows[0][columns[0]]
            else:
                return 0

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    print Hive().query("SHOW TABLES")
