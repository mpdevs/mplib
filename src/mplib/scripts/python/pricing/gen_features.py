# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common import time_elapse
from mplib.IO import Hive, PostgreSQL
from mplib import *


@time_elapse
def gen_attribute_dictionary():
    sql = """
    DROP TABLE IF EXISTS elengjing_price.attr_value_text;
    CREATE TABLE IF NOT EXISTS elengjing_price.attr_value_text(
        categoryid STRING,
        attr STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY'\t';
    LOAD DATA LOCAL INPATH '/home/data/attr_value.csv' OVERWRITE INTO TABLE elengjing_price.attr_value_text;


    DROP TABLE IF EXISTS elengjing_price.attr_value;
    CREATE TABLE elengjing_price.attr_value
    STORED AS ORC
    AS
    SELECT *
    FROM elengjing_price.attr_value_text;
    """
    Hive("idc").execute(sql)


@time_elapse
def gen_attribute_matrix(category_id, keep_tmp=False):
    sql = """
    DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_{category_id};
    CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_{category_id}(
        itemid BIGINT
    )
    CLUSTERED BY (itemid) INTO 997 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE elengjing_price.attr_unique_itemid_{category_id}
    SELECT itemid
    FROM elengjing.women_clothing_item_attr_v1
    WHERE categoryid = {category_id}
    AND attrname != '品牌' GROUP BY itemid;

    DROP TABLE IF EXISTS elengjing_price.item_attr_{category_id};
    CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_{category_id}(
        itemid BIGINT,
        attr STRING
    )
    CLUSTERED BY (itemid) INTO 997 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE elengjing_price.item_attr_{category_id}
    SELECT
        itemid,
        CONCAT(attrname, ":", attrvalue)
    FROM elengjing.women_clothing_item_attr_v1
    WHERE categoryid = {category_id}
    AND attrname != '品牌';

    DROP TABLE IF EXISTS elengjing_price.item_attr_1_{category_id};
    CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_{category_id}(
        itemid BIGINT,
        attr STRING
    )
    CLUSTERED BY (itemid) INTO 997 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE elengjing_price.item_attr_1_{category_id}
    SELECT
        itemid,
        attr
    FROM elengjing_price.item_attr_{category_id}
    GROUP BY
        itemid,
        attr;

    DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_{category_id};
    CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_{category_id}(
        itemid BIGINT,
        attr STRING
    )
    CLUSTERED BY (itemid) INTO 997 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE elengjing_price.attr_itemid_value_{category_id}
    SELECT
        t1.itemid,
        t2.attr
    FROM elengjing_price.attr_unique_itemid_{category_id} AS t1
    CROSS JOIN elengjing_price.attr_value AS t2
    WHERE t2.categoryid = {category_id};

    DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_{category_id};
    CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_{category_id}(
        itemid BIGINT,
        attr STRING,
        data INT
    )
    CLUSTERED BY (itemid) INTO 997 BUCKETS
    STORED AS ORC;

    SET mapred.reduce.tasks = 1000;

    INSERT INTO TABLE elengjing_price.attr_itemid_value_data_{category_id}
    SELECT
        t1.*,
        IF(NVL(t2.itemid, 0) = 0, 0, 1)
    FROM elengjing_price.attr_itemid_value_{category_id} AS t1
    LEFT JOIN elengjing_price.item_attr_1_{category_id} AS t2
    ON t1.itemid = t2.itemid
    AND t1.attr = t2.attr;

    DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_{category_id};
    CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_{category_id}(
        itemid BIGINT,
        attr STRING,
        data STRING
    )
    CLUSTERED BY (itemid) INTO 997 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_{category_id}
    SELECT
        itemid,
        CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
        CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
    FROM (
        SELECT
            itemid,
            attr,
            data,
            row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
        FROM elengjing_price.attr_itemid_value_data_{category_id}
    ) AS a
    GROUP BY itemid;
    """.format(category_id=category_id)
    Hive("idc").execute(sql)

    if not keep_tmp:
        sql = """
        USE elengjing_price;
        DROP TABLE attr_itemid_value_{category_id};
        DROP TABLE attr_itemid_value_data_{category_id};
        DROP TABLE attr_unique_itemid_{category_id};
        DROP TABLE item_attr_1_{category_id};
        DROP TABLE item_attr_{category_id};
        """.format(category_id=category_id)
        Hive("idc").execute(sql)


@time_elapse
def gen_train_features(category_id, is_event=False, date="WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'"):
    event = "event" if is_event else "daily"
    sql = """
    USE elengjing_price;
    DROP TABLE IF EXISTS train_{category_id}_{event};
    CREATE TABLE train_{category_id}_{event} AS
    SELECT
        i.itemid,
        CONCAT_WS(',',
        CAST(i.avg_price AS STRING),
        CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING),
        CAST(shop.level AS STRING),
        CAST(shop.favor AS STRING),
        CAST(s.all_spu AS STRING),
        CAST(s.all_sales_rank AS STRING),
        CAST(sc.category_spu AS STRING),
        CAST(sc.category_sales_rank AS STRING),
        CAST(s.shop_avg_price AS STRING),
        CAST(s.shop_avg_price_rank AS STRING),
        CAST(sc.category_avg_price AS STRING),
        CAST(sc.category_avg_price_rank AS STRING),
        v.data
      ) AS data
    FROM
      (SELECT
        shopid, itemid,
        MAX(listeddate) AS listeddate,
        CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
      FROM elengjing.women_clothing_item
      {date_filter}
      AND categoryid = {category_id}
      GROUP BY
        shopid, itemid) AS i
    JOIN
      (SELECT
        shopid,
        COUNT(DISTINCT itemid) AS all_spu,
        CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
        ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
        ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
      FROM elengjing.women_clothing_item
      {date_filter}
      GROUP BY
        shopid) AS s
    ON i.shopid = s.shopid
    JOIN
      (SELECT
        shopid,
        COUNT(DISTINCT itemid) AS category_spu,
        ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
        ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
        CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
      FROM elengjing.women_clothing_item
      {date_filter}
      AND categoryid = {category_id}
      GROUP BY
        shopid) AS sc
    ON i.shopid = sc.shopid
    JOIN elengjing_base.shop AS shop
    ON i.shopid = shop.shopid
    JOIN elengjing_price.attr_itemid_value_matrix_{category_id} AS v
    ON i.itemid = v.itemid
    WHERE shop.level IS NOT NULL
    AND shop.favor IS NOT NULL
    AND i.avg_price BETWEEN 10 AND 10000;
    """.format(category_id=category_id, date_filter=date, event=event)
    Hive("idc").execute(sql)


def get_all_category():
    lines = PostgreSQL("v2_uat").query("SELECT id FROM category WHERE industry_id = 16 AND is_leaf = 't'")
    return [str(line.get("id")) for line in lines]


def batch(date_filter, tag_dict=False, tagging=False, keep_tmp=False, feature=False, is_event=False):
    from datetime import datetime
    if tag_dict:
        print("{0} 开始处理标签字典".format(datetime.now()))
        gen_attribute_dictionary()
    category_list = get_all_category()
    for idx, category in enumerate(category_list):
        if tagging:
            print("{0} 开始处理品类 {1} ({2}/{3})的标签数据".format(datetime.now(), category, idx + 1, len(category_list)))
            gen_attribute_matrix(category_id=category, keep_tmp=keep_tmp)
        if feature:
            event = "活动定价" if is_event else "日常定价"
            print("{0} 开始生成品类 {1} ({2}/{3}){4}的训练数据".format(
                datetime.now(), category, idx + 1, len(category_list), event))
            gen_train_features(category, is_event, date_filter)


if __name__ == "__main__":
    batch_dict = dict(
        tag_dict=False,
        tagging=False,
        feature=True,
        keep_tmp=False,
        is_event=True,
        date_filter="WHERE daterange BETWEEN '2016-05-25' AND '2016-05-31'"
    )
    batch(**batch_dict)
