# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common import time_elapse
from mplib.IO import Hive
from datetime import datetime


CATEGORY_IDS = [
    162104,
    50008904,
    50000697,
    162201,
    50008905,
    50008897,
    50013194,
    50008898,
    50010850,
    50008899,
    162116,
    121412004,
    50013196,
    50008900,
    50007068,
    50011277,
    162205,
    50008901,
    162103,
    1623,
    50000671
]

SHOP_ID = 73401272


def create_table(**kwargs):
    sql = """
        USE elengjing_tj;
        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case};
        CREATE TABLE t_shop_{shop_id}_case_{case} (
            category_id BIGINT,
            customer_shop_id BIGINT,
            customer_item_id BIGINT,
            customer_platform_id BIGINT,
            target_shop_id BIGINT,
            target_item_id BIGINT,
            target_platform_id BIGINT
        )
        CLUSTERED BY (customer_item_id) INTO 113 BUCKETS
        STORED AS ORC;
        """.format(**kwargs)
    Hive("idc").execute(sql)


@time_elapse
def insert_by_category(**kwargs):
    sql = """
        USE elengjing_tj;
        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case}_customer;
        CREATE TABLE t_shop_{shop_id}_case_{case}_customer (
            shopid BIGINT,
            categoryid BIGINT,
            itemid BIGINT,
            platformid BIGINT,
            avg_price FLOAT
        )
        CLUSTERED BY (itemid) INTO 113 BUCKETS
        STORED AS ORC;


        INSERT INTO TABLE t_shop_{shop_id}_case_{case}_customer
        SELECT
                shopid,
                categoryid,
                itemid,
                platformid,
                CASE
                    WHEN NVL(SUM(salesqty), 0) = 0
                    THEN 0
                    ELSE 1.0 * NVL(SUM(salesamt), 0) / SUM(salesqty)
                END AS avg_price
            FROM elengjing.women_clothing_item
            WHERE shopid = {shop_id}
            AND categoryid = {category_id}
            GROUP BY
                shopid,
                categoryid,
                itemid,
                platformid;


        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case}_target;
        CREATE TABLE t_shop_{shop_id}_case_{case}_target (
            shopid BIGINT,
            categoryid BIGINT,
            itemid BIGINT,
            platformid BIGINT,
            avg_price FLOAT
        )
        CLUSTERED BY (itemid) INTO 113 BUCKETS
        STORED AS ORC;

        INSERT INTO TABLE t_shop_{shop_id}_case_{case}_target
        SELECT
            b.shopid,
            b.categoryid,
            b.itemid,
            b.platformid,
            b.avg_price
        FROM (
            SELECT
                a.shopid,
                a.categoryid,
                a.itemid,
                a.platformid,
                a.avg_price,
                1.0 * SUM(a.qty) OVER (ORDER BY a.qty DESC, a.qty ROWS UNBOUNDED PRECEDING) / t.qty AS c_ratio
            FROM (
                SELECT
                    shopid,
                    categoryid,
                    itemid,
                    platformid,
                    CASE
                        WHEN NVL(SUM(salesqty), 0) = 0
                        THEN 0
                        ELSE 1.0 * NVL(SUM(salesamt), 0) / SUM(salesqty)
                    END AS avg_price,
                    SUM(salesqty) AS qty
                FROM elengjing.women_clothing_item
                WHERE shopid != {shop_id}
                AND categoryid = {category_id}
                GROUP BY
                    shopid,
                    categoryid,
                    itemid,
                    platformid
                ORDER BY qty DESC
            ) AS a
            CROSS JOIN (
                SELECT
                    SUM(salesqty) AS qty
                FROM elengjing.women_clothing_item
                WHERE shopid != {shop_id}
                AND categoryid = {category_id}
            ) AS t
        ) AS b
        WHERE b.c_ratio <= 0.8;


        INSERT INTO elengjing_tj.t_shop_{shop_id}_case_{case}
        SELECT
            c.categoryid AS category_id,
            c.shopid AS customer_shop_id,
            c.itemid AS customer_item_id,
            c.platformid AS customer_platform_id,
            t.shopid AS target_shop_id,
            t.itemid AS target_item_id,
            t.platformid AS target_platform_id
        FROM t_shop_{shop_id}_case_{case}_customer AS c
        CROSS JOIN t_shop_{shop_id}_case_{case}_target AS t
        WHERE t.avg_price BETWEEN 0.9 * c.avg_price AND 1.1 * c.avg_price;
        """.format(**kwargs)
    Hive("idc").execute(sql)


@time_elapse
def transform(**kwargs):
    sql = """
        USE elengjing_tj;
        DROP TABLE IF EXISTS shop_{shop_id}_case_{case};
        ALTER TABLE t_shop_{shop_id}_case_{case} RENAME TO shop_{shop_id}_case_{case};
        DROP TABLE IF EXISTS shop_{shop_id}_case_{case}_item;
        CREATE TABLE shop_{shop_id}_case_{case}_item AS (
        SELECT
            category_id,
            customer_item_id AS item_id,
            count(*) as cnt
        FROM elengjing_tj.shop_{shop_id}_case_{case}
        GROUP BY
            category_id,
            item_id
        UNION ALL
        SELECT
            category_id,
            target_item_id AS item_id,
            count(*) as cnt
        FROM elengjing_tj.shop_{shop_id}_case_{case}
        GROUP BY
            category_id,
            item_id
        );
        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case}_target;
        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case}_customer;
        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case};
        DROP TABLE IF EXISTS t_shop_{shop_id}_case_{case}_scope;
        """.format(**kwargs)
    Hive("idc").execute(sql)


@time_elapse
def update_tj(**kwargs):
    category_ids = CATEGORY_IDS
    create_table(**kwargs)

    for index, category_id in enumerate(category_ids):
        kwargs.update(dict(category_id=category_id))
        print("{0} executing category_id = {1} {2}/{3}".format(
            datetime.now(), category_id, index + 1, len(category_ids)))
        insert_by_category(**kwargs)

    transform(**kwargs)


def batch(shop_ids=None):
    if not shop_ids:
        return

    for shop_id in shop_ids:
        update_tj(shop_id=shop_id, case="b")


if __name__ == "__main__":
    batch(shop_ids=[SHOP_ID])
