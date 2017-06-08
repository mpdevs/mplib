# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.IO import MySQL, Hive
from datetime import datetime
from math import ceil


def get_table_definition(table_name):
    sql = """
    SELECT
        a.COLUMN_NAME,
        a.TYPE_NAME
    FROM hive.SDS AS c
    JOIN hive.TBLS AS b
    ON b.SD_ID=c.SD_ID
    JOIN hive.COLUMNS_V2 AS a
    ON a.CD_ID=c.CD_ID
    WHERE b.tbl_name='{0}'
    """.format(table_name)
    definition = [[line.get("COLUMN_NAME"), line.get("TYPE_NAME")] for line in MySQL("local_hive").query(sql)]
    return definition


def get_table_count(table_name):
    sql = "SELECT COUNT(*) AS cnt FROM das.{0}".format(table_name)
    return Hive("local").query(sql)[0].get("cnt")


def rebuild_table(table_name):
    count = get_table_count(table_name)
    if not count:
        print("no rows in das.{0}".format(table_name))
        return

    definition = ",\n".join(["{0} {1}".format(d[0], d[1]) for d in get_table_definition(table_name)])
    clusters = int(ceil(count / 1e7)) * 113
    time_stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    select_columns = ",".join([d[0] for d in get_table_definition(table_name)])
    print(datetime.now())
    sql = """
    DROP TABLE IF EXISTS {0}_{1};
    CREATE TABLE {0}_{1} (
      {2}
    )CLUSTERED BY (id) INTO {3} BUCKETS
    STORED AS ORC;
    """.format(table_name, time_stamp, definition, clusters)
    Hive("local").execute(sql)
    print(datetime.now())
    sql = """
    INSERT INTO TABLE das.{0}_{1}
    SELECT {2}
    FROM das.{0};
    """.format(table_name, time_stamp, select_columns)
    Hive("local").execute(sql)
    print(datetime.now())
    sql = """
    DROP TABLE das.{0};
    ALTER TABLE das.{0}_{1} RENAME TO das.{0};
    """.format(table_name, time_stamp)
    Hive("local").execute(sql)
    print(datetime.now())


if __name__ == "__main__":
    rebuild_table("daneng_20170531_2002to2004_withinfo")
