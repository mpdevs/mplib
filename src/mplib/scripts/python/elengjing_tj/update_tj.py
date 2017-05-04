# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.IO import PostgreSQL, Hive


def get_registered_shop():
    sql = """
    SELECT DISTINCT s.id
    FROM elengjing.public.user AS u
    JOIN elengjing.public.shop AS s
    ON u.shop_id = s.id
    WHERE s.is_validated = 't'
    """
    return PostgreSQL(env="v2_uat").query(sql)


def update_tj(shop_id):
    sql = """
    USE elengjing_tj;
    DROP TABLE IF EXISTS t_shop_{id};
    CREATE TABLE t_shop_{id} AS
    SELECT
        c.categoryid AS category_id,
        c.categoryname AS category_name,
        c.shopid AS customer_shop_id,
        c.shopname AS customer_shop_name,
        c.itemid AS customer_item_id,
        c.itemname AS customer_item_name,
        t.shopid AS target_shop_id,
        t.shopname AS target_shop_name,
        t.itemid AS target_item_id,
        t.itemname AS target_item_name
    FROM (
        SELECT
            shopid,
            shopname,
            categoryid,
            categoryname,
            itemid,
            itemname,
            CASE
                WHEN NVL(SUM(salesqty), 0) = 0
                THEN 0
                ELSE 1.0 * NVL(SUM(salesamt), 0) / SUM(salesqty)
            END AS avg_price
        FROM elengjing.women_clothing_item
        WHERE shopid = {id}
        GROUP BY
            shopid,
            shopname,
            categoryid,
            categoryname,
            itemid,
            itemname
    ) AS c
    CROSS JOIN (
        SELECT
            shopid,
            shopname,
            categoryid,
            categoryname,
            itemid,
            itemname,
            CASE
                WHEN NVL(SUM(salesqty), 0) = 0
                THEN 0
                ELSE 1.0 * NVL(SUM(salesamt), 0) / SUM(salesqty)
            END AS avg_price
        FROM elengjing.women_clothing_item
        WHERE shopid != {id}
        GROUP BY
            shopid,
            shopname,
            categoryid,
            categoryname,
            itemid,
            itemname
    ) AS t
    WHERE c.categoryid = t.categoryid
    AND t.avg_price BETWEEN 0.8 * c.avg_price AND 1.2 *c.avg_price;
    DROP TABLE IF EXISTS shop_{id};
    ALTER TABLE t_shop_{id} RENAME TO shop_{id};
    """
    Hive(env="idc").execute(sql.format(**shop_id))


def batch(shop_ids=None):
    if not shop_ids:
        shop_ids = get_registered_shop()

    for shop_id in shop_ids:
        update_tj(shop_id)

if __name__ == "__main__":
    batch()
