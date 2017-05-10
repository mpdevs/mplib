# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common import time_elapse
from mplib.IO import PostgreSQL, Hive
from datetime import datetime


def get_registered_shop():
    sql = """
    SELECT DISTINCT s.id
    FROM elengjing.public.user AS u
    JOIN elengjing.public.shop AS s
    ON u.shop_id = s.id
    WHERE s.is_validated = 't'
    """
    return PostgreSQL(env="v2_uat").query(sql)


def get_shop_category(**kwargs):
    sql = """
    SELECT
        DISTINCT categoryid
    FROM elengjing.women_clothing_item
    WHERE shopid = {shop_id}
    """.format(**kwargs)
    return list(map(lambda x: x[0], Hive("idc").query(sql, to_dict=False)))


def create_table(**kwargs):
    sql = """
        USE elengjing_tj;
        DROP TABLE IF EXISTS t_shop_{shop_id};
        CREATE TABLE t_shop_{shop_id} (
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
        DROP TABLE IF EXISTS t_shop_{shop_id}_customer;
        CREATE TABLE t_shop_{shop_id}_customer (
            shopid BIGINT,
            categoryid BIGINT,
            itemid BIGINT,
            platformid BIGINT,
            avg_price FLOAT
        )
        CLUSTERED BY (itemid) INTO 113 BUCKETS
        STORED AS ORC;

        INSERT INTO TABLE t_shop_{shop_id}_customer
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

        DROP TABLE IF EXISTS t_shop_{shop_id}_target;
        CREATE TABLE t_shop_{shop_id}_target (
            shopid BIGINT,
            categoryid BIGINT,
            itemid BIGINT,
            platformid BIGINT,
            avg_price FLOAT
        )
        CLUSTERED BY (itemid) INTO 113 BUCKETS
        STORED AS ORC;

        DROP TABLE IF EXISTS t_shop_{shop_id}_scope;
        CREATE TABLE t_shop_{shop_id}_scope AS
        SELECT
            shopid AS shop_id,
            rank,
            CASE
                WHEN rank <= 1000
                THEN 1
                ELSE 0
            END AS is_top
        FROM (
            SELECT
                shopid,
                ROW_NUMBER() OVER(ORDER BY
                    CASE
                        WHEN NVL(SUM(salesqty), 0) = 0
                        THEN 0
                        ELSE 1.0 * NVL(SUM(salesamt), 0) / SUM(salesqty)
                    END DESC
                ) AS rank
            FROM elengjing.women_clothing_item
            GROUP BY
                shopid
        ) AS t;

        INSERT INTO t_shop_{shop_id}_target
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
            WHERE shopid IN (
                SELECT s.shop_id
                FROM t_shop_{shop_id}_scope AS s
                CROSS JOIN (
                    SELECT
                        CASE
                            WHEN rank <= 1000
                            THEN 1000
                            ELSE rank + 500
                        END AS rank
                    FROM t_shop_{shop_id}_scope
                    WHERE shop_id = {shop_id}
                ) AS f
                WHERE s.rank <= f.rank
            )
            AND categoryid = {category_id}
            GROUP BY
                shopid,
                categoryid,
                itemid,
                platformid;

        INSERT INTO elengjing_tj.t_shop_{shop_id}
        SELECT
            c.categoryid AS category_id,
            c.shopid AS customer_shop_id,
            c.itemid AS customer_item_id,
            c.platformid AS customer_platform_id,
            t.shopid AS target_shop_id,
            t.itemid AS target_item_id,
            t.platformid AS target_platform_id
        FROM t_shop_{shop_id}_customer AS c
        CROSS JOIN t_shop_{shop_id}_target AS t
        WHERE t.avg_price BETWEEN 0.9 * c.avg_price AND 1.1 *c.avg_price;
        """.format(**kwargs)
    Hive("idc").execute(sql)


@time_elapse
def transform(**kwargs):
    sql = """
        USE elengjing_tj;
        DROP TABLE IF EXISTS shop_{shop_id};
        ALTER TABLE t_shop_{shop_id} RENAME TO shop_{shop_id};
        DROP TABLE IF EXISTS shop_{shop_id}_item;
        CREATE TABLE shop_{shop_id}_item AS (
        SELECT
            category_id,
            customer_item_id AS item_id,
            count(*) as cnt
        FROM elengjing_tj.shop_{shop_id}
        GROUP BY
            category_id,
            item_id
        UNION ALL
        SELECT
            category_id,
            target_item_id AS item_id,
            count(*) as cnt
        FROM elengjing_tj.shop_{shop_id}
        GROUP BY
            category_id,
            item_id
        );
        DROP TABLE IF EXISTS t_shop_{shop_id}_target;
        DROP TABLE IF EXISTS t_shop_{shop_id}_customer;
        DROP TABLE IF EXISTS t_shop_{shop_id};
        DROP TABLE IF EXISTS t_shop_{shop_id}_scope;
        """.format(**kwargs)
    Hive("idc").execute(sql)


@time_elapse
def update_tj(**kwargs):
    category_ids = get_shop_category(**kwargs)
    create_table(**kwargs)

    for index, category_id in enumerate(category_ids):
        kwargs.update(dict(category_id=category_id))
        print("{0} executing category_id = {1} {2}/{3}".format(datetime.now(), category_id, index, len(category_ids)))
        insert_by_category(**kwargs)

    transform(**kwargs)


def batch(shop_ids=None):
    if not shop_ids:
        shop_ids = get_registered_shop()

    for shop_id in shop_ids:
        update_tj(shop_id=shop_id)

if __name__ == "__main__":
    # print(get_registered_shop())
    batch([113462750])
    # print(get_shop_category(shop_id=122667442))
