# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.IO import Hive
from mplib.pricing.helper import get_all_category
from mplib.common import time_elapse


@time_elapse
def create(is_event=True, show_sql=False):
    sql = """
    USE elengjing_price;
    CREATE TABLE predict_{0} (
        itemid BIGINT,
        categoryid BIGINT,
        price DECIMAL(20, 2)
    )
    STORED AS ORC;
    """.format("event" if is_event else "daily")
    if show_sql:
        print(sql)
    else:
        Hive("idc").execute(sql)


@time_elapse
def insert(category_id, is_event, show_sql=False):
    raw_sql = """
    USE elengjing_price;
    INSERT INTO predict_{0}
    SELECT
        itemid,
        {1} AS categoryid,
        CASE
            WHEN price IS NULL OR price <= 0 THEN -1
            ELSE price
        END AS price
    FROM predict_{1}_{0};
    """
    if isinstance(category_id, list):
        sql = ";\n".join([raw_sql.format("event" if is_event else "daily", c) for c in category_id])
    else:
        sql = raw_sql.format("event" if is_event else "daily", category_id)
    if show_sql:
        print(sql)
    else:
        Hive("idc").execute(sql)


@time_elapse
def drop(is_event, show_sql=False):
    sql = """
    USE elengjing_price;
    DROP TABLE IF EXISTS predict_{0};
    """.format("event" if is_event else "daily")
    if show_sql:
        print(sql)
    else:
        Hive("idc").execute(sql)


def main(is_event, category_id, show_sql, create_table, drop_table):
    if drop_table:
        drop(is_event=is_event, show_sql=show_sql)
    if create_table:
        create(is_event=is_event, show_sql=show_sql)
    insert(category_id=category_id, is_event=is_event, show_sql=show_sql)


if __name__ == "__main__":
    config = dict(
        create_table=True,
        drop_table=True,
        is_event=False,
        category_id=get_all_category(),
        show_sql=True
    )
    main(**config)
