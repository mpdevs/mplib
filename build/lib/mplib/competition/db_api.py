# coding: utf-8
# __author__ = "John"
from __future__ import unicode_literals
from mplib.common.settings import MYSQL_SETTINGS
from datetime import datetime
from mplib.IO import MySQL
from math import ceil
import pandas


def connect_mysql(db):
    connection = MySQL(env="mpportal")

    return


def generate_shop_filter(shop_id):
    shop_id_filter = ""
    if shop_id:
        shop_id_filter = " shop_id = {0} AND ".format(shop_id)
    return shop_id_filter


def get_item_attributes(db="mp_women_clothing", limits=""):
    sql = """
    SELECT
        ItemID,
        TaggedItemAttr
    FROM {0}
    WHERE TaggedItemAttr IS NOT NULL
    AND TaggedItemAttr != ''
    AND TaggedItemAttr LIKE ',%' {1};
    """
    return pandas.read_sql_query(sql.format("TaggedItemAttr", limits), get_mysql_connect(db))


def get_result_data(cid, db="mp_women_clothing"):
    sql = """
    SELECT *
    FROM tagged_competitive_items
    WHERE CategoryId = {0};
    """
    return pandas.read_sql_query(sql.format(cid), get_mysql_connect(db))


def get_training_data(cid, db="mp_women_clothing"):
    sql = """
    SELECT DISTINCT
        a1.TaggedItemAttr AS attr1,
        a2.TaggedItemAttr AS attr2,
        c.score,
        a1.ItemID,
        a2.ItemID
    FROM tagged_competitive_items c
    JOIN TaggedItemAttr a1
    ON a1.ItemID = c.SourceItemID
    JOIN TaggedItemAttr a2
    ON a2.ItemID = c.TargetItemID
    WHERE c.CategoryID = {0}
    AND a1.TaggedItemAttr LIKE ',%'
    AND a2.TaggedItemAttr LIKE ',%';
    """
    return pandas.read_sql_query(sql.format(cid), get_mysql_connect(db))


def get_new_training_data(cid, db="mp_women_clothing"):
    sql = """
    SELECT DISTINCT
        a1.TaggedItemAttr AS attr1,
        a2.TaggedItemAttr AS attr2,
        c.score,
        a1.ItemID,
        a2.ItemID
    FROM tagged_competitive_items c
    JOIN itemmonthlysales_201607 a1
    ON a1.ItemID = c.SourceItemID
    JOIN itemmonthlysales_201607 a2
    ON a2.ItemID = c.TargetItemID
    WHERE c.CategoryID = {0}
    AND a1.TaggedItemAttr LIKE ',%'
    AND a2.TaggedItemAttr LIKE ',%';
    """
    return pandas.read_sql_query(sql.format(cid), get_mysql_connect(db))


def get_customer_shop_items(db, table, category_id, date_range, shop_id):
    sql = """
    SELECT DISTINCT
        ItemID,
        TaggedItemAttr,
        DiscountPrice,
        CategoryID,
        DateRange
    FROM {0}
    WHERE {1} CategoryID = {2}
    AND DateRange = '{3}'
    AND TaggedItemAttr LIKE ',%'
    ORDER BY RAND()
    LIMIT 3000;
    """
    shop_filter = ""
    if shop_id:
        shop_filter = " ShopID = {0} AND ".format(shop_id)
    return pandas.read_sql_query(sql.format(table, shop_filter, category_id, date_range), get_mysql_connect(db))


def get_competitor_shop_items(db, table, category_id, date_range):
    sql = """
    SELECT DISTINCT
        ItemID,
        TaggedItemAttr,
        DiscountPrice,
        CategoryID,
        DateRange
    FROM {0}
    WHERE CategoryID = {1}
    AND DateRange = '{2}'
    AND TaggedItemAttr LIKE ',%'
    ORDER BY RAND()
    LIMIT 3000;
    """
    return pandas.read_sql_query(sql.format(table, category_id, date_range), get_mysql_connect(db))


def delete_score(db, table, category_id, date_range, shop_id):
    sql = """
    DELETE FROM {0}.{1}
    WHERE {2} CategoryID = {3}
    AND DateRange = '{4}';
    """
    db_connection = MySQL()
    db_connection.execute(sql.format(db, table, generate_shop_filter(shop_id), category_id, date_range))
    return


def set_scores(db, table, args):
    """
    :param db:
    :param table:
    :param args:
    :return:
    """
    sql = """
    INSERT INTO {0}.{1} (ShopID, SourceItemID, TargetItemID, Score ,DateRange, CategoryID, RelationType, Status)
    VALUES (%s, %s, %s, %s, %s, %s, 1, 1);
    """
    db_connection = MySQL()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in range(batch):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=sql.format(db, table), args=data)
    return


def get_max_date_range(db, table):
    """
    因为数据源和数据目的地都有年月控制的字段，所以需要对数据进行删选，同月份数据有可比性，不同月份则没有
    :param db:
    :param table:
    :return:
    """
    sql = """
    SELECT
        MAX(DateRange) AS DateRange
    FROM {0}.{1};
    """
    return pandas.read_sql_query(sql.format(db, table), get_mysql_connect()).values[0][0]


def get_essential_dimensions(db="mp_women_clothing", threshold=0.75, confidence=5):
    """
    必要维度法需要的数据， 基本把潜在竞品筛完
    :param db:
    :param threshold:
    :param confidence:
    :return: OrderedDict
    """
    sql = """
    SELECT
        CategoryID,
        AttrName,
        AttrValue
    FROM essential_dimensions
    WHERE MoreThanTwoNegPercent >= {0}
    AND ConfidenceLevel >= {1};
    """
    return pandas.read_sql_query(sql.format(threshold, confidence), get_mysql_connect(db))


if __name__ == "__main__":
    _industry = "mp_women_clothing"
    _source_table = "itemmonthlysales2015"
    _target_table = "itemmonthlyrelation_2015"
    _shop_id = 66098091
    _date_range = "2015-12-01"
    _category_id = 1623

    print "{0} start testing get_training_data".format(datetime.now())
    r = get_training_data(1623)
    print "get_training_data row count={0}".format(r.values.shape[0])

    print "{0} start testing get_customer_shop_items".format(datetime.now())
    r = get_customer_shop_items(db=_industry, table=_source_table, shop_id=66098091, date_range=_date_range,
                                category_id=_category_id)
    print "get_customer_shop_items row count={0}".format(r.values.shape[0])

    # print u"{0} start testing set_scores".format(datetime.now())
    # batch_data = [(1, 1, 1, 0, datetime.now().strftime("%Y-%m-%d")),
    #               (0, 0, 0, 1, datetime.now().strftime("%Y-%m-%d"))]
    # set_scores(db="mp_women_clothing", table="itemmonthlyrelation_2016", args=batch_data)

    print "{0} start testing get_max_date_range".format(datetime.now())
    dr = get_max_date_range(db=_industry, table=_source_table)
    print "date_range={0}".format(dr)

    print "{0} start testing get_essential_dimensions".format(datetime.now())
    r = get_essential_dimensions(db=_industry)
    print "get_essential_dimensions row count={0}".format(r.values.shape[0])
