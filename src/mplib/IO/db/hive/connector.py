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
            s = s.strip()
            if s:
                try:
                    self.cursor.execute(smart_encode(s))
                except:
                    print(s)
        self.close()

    def close(self):
        self.cursor.close()
        self.conn.close()

    @staticmethod
    def get_env_dict():
        return get_env_dict()

    @staticmethod
    def show_env_dict():
        from pprint import pprint
        pprint(get_env_dict())


if __name__ == "__main__":
    from pprint import pprint
    Hive(env="idc").execute("CREATE TABLE alahubake(id string); DROP TABLE alahubake;")
    pprint(Hive(env="idc").query("SHOW TABLES"))
    pprint(Hive(env="idc").query("SELECT dateid, daterange FROM dimdate LIMIT 1", to_dict=False))
    Hive.show_env_dict()
    sql = """
        WITH market_base AS (
        SELECT
            i.itemid,
            i.salesamt,
            i.salesqty,
            d.yearmonth AS date_range,
            CASE
                WHEN i.daterange BETWEEN '2016-07-11' AND '2016-12-17'
                THEN 1
                ELSE 0
            END AS is_current
        FROM elengjing.dimdate AS d
        LEFT JOIN elengjing.women_clothing_item AS i
        ON d.daterange = i.daterange

        WHERE (d.daterange BETWEEN '2016-07-11' AND '2016-12-17' OR d.daterange BETWEEN '2015-07-11' AND '2015-12-17')
        AND i.shopid = 113462750
        AND i.platformid = 7011
    ), market_current AS (
        SELECT
            NVL(SUM(salesamt), 0) AS kpi,
            date_range
        FROM market_base
        WHERE is_current = 1
        GROUP BY date_range
    ), market_last AS (
        SELECT
            NVL(SUM(salesamt), 0) AS kpi,
            date_range
        FROM market_base
        WHERE is_current = 0
        GROUP BY date_range
    )
    SELECT
        CASE
            WHEN NVL(market_last.kpi, 0) = 0
            THEN 0
            ELSE ROUND(100.0 * market_current.kpi / market_last.kpi - 100, 2)
        END AS increase_ratio,
        market_current.date_range
    FROM  market_current
    LEFT JOIN  market_last
    ON market_last.date_range = market_current.date_range
    ORDER BY market_current.date_range
    """
    Hive(env="idc").query(sql)
