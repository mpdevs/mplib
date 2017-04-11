# coding: utf-8
# __author__ = "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.IO import MySQL, get_mysql_connect
from math import ceil
import pandas


def get_item_attributes(db="mp_women_clothing", limits=""):
    sql = """
    SELECT
        ItemID,
        TaggedItemAttr
    FROM {2}.{0}
    WHERE TaggedItemAttr IS NOT NULL
    AND TaggedItemAttr != ''
    AND TaggedItemAttr LIKE ',%' {1};
    """
    return pandas.read_sql_query(sql.format("TaggedItemAttr", limits, db), get_mysql_connect(env="mpportal"))


def get_result_data(cid, db="mp_women_clothing"):
    sql = """
    SELECT *
    FROM {1}.tagged_competitive_items
    WHERE CategoryId = {0};
    """
    return pandas.read_sql_query(sql.format(cid, db), get_mysql_connect(env="mpportal"))


def get_training_data(cid, db="mp_women_clothing"):
    sql = """
    SELECT DISTINCT
        a1.TaggedItemAttr AS attr1,
        a2.TaggedItemAttr AS attr2,
        c.score,
        a1.ItemID,
        a2.ItemID
    FROM {1}.tagged_competitive_items AS c
    JOIN {1}.TaggedItemAttr AS a1
    ON a1.ItemID = c.SourceItemID
    JOIN {1}.TaggedItemAttr AS a2
    ON a2.ItemID = c.TargetItemID
    WHERE c.CategoryID = {0}
    AND a1.TaggedItemAttr LIKE ',%'
    AND a2.TaggedItemAttr LIKE ',%';
    """
    return pandas.read_sql_query(sql.format(cid, db), get_mysql_connect(env="mpportal"))


def get_new_training_data(cid, db="mp_women_clothing"):
    sql = """
    SELECT DISTINCT
        a1.TaggedItemAttr AS attr1,
        a2.TaggedItemAttr AS attr2,
        c.score,
        a1.ItemID,
        a2.ItemID
    FROM {1}.tagged_competitive_items c
    JOIN {1}.itemmonthlysales_201607 a1
    ON a1.ItemID = c.SourceItemID
    JOIN {1}.itemmonthlysales_201607 a2
    ON a2.ItemID = c.TargetItemID
    WHERE c.CategoryID = {0}
    AND a1.TaggedItemAttr LIKE ',%'
    AND a2.TaggedItemAttr LIKE ',%';
    """
    return pandas.read_sql_query(sql.format(cid, db), get_mysql_connect(env="mpportal"))


def get_customer_shop_items(db, table, category_id, date_range, shop_id):
    sql = """
    SELECT DISTINCT
        ItemID,
        TaggedItemAttr,
        DiscountPrice,
        CategoryID,
        DateRange
    FROM {4}.{0}
    WHERE {1} CategoryID = {2}
    AND DateRange = '{3}'
    AND TaggedItemAttr LIKE ',%'
    ORDER BY RAND()
    LIMIT 3000;
    """
    shop_filter = ""
    if shop_id:
        shop_filter = " ShopID = {0} AND ".format(shop_id)
    sql = sql.format(table, shop_filter, category_id, date_range, db)
    return pandas.read_sql_query(sql, get_mysql_connect(env="mpportal"))


def get_competitor_shop_items(db, table, category_id, date_range):
    sql = """
    SELECT DISTINCT
        ItemID,
        TaggedItemAttr,
        DiscountPrice,
        CategoryID,
        DateRange
    FROM {3}.{0}
    WHERE CategoryID = {1}
    AND DateRange = '{2}'
    AND TaggedItemAttr LIKE ',%'
    ORDER BY RAND()
    LIMIT 3000;
    """
    return pandas.read_sql_query(sql.format(table, category_id, date_range, db), get_mysql_connect(env="mpportal"))


def generate_shop_filter(shop_id):
    shop_id_filter = ""
    if shop_id:
        shop_id_filter = " shop_id = {0} AND ".format(shop_id)
    return shop_id_filter


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
    INSERT INTO {0}.{1}
        (ShopID, SourceItemID, TargetItemID, Score ,DateRange, CategoryID, RelationType, Status)
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
    return pandas.read_sql_query(sql.format(db, table), get_mysql_connect(env="mpportal")).values[0][0]


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
    FROM {2}.essential_dimensions
    WHERE MoreThanTwoNegPercent >= {0}
    AND ConfidenceLevel >= {1};
    """
    return pandas.read_sql_query(sql.format(threshold, confidence, db), get_mysql_connect(env="mpportal"))


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
    FROM {0}.{1};"""
    return pandas.read_sql_query(sql.format(db, table), get_mysql_connect(env="mpportal")).values[0][0]


def get_attribute_meta(db=u"mp_women_clothing"):
    sql = """
    SELECT
        a.CID,
        a.Attrname,
        a.DisplayName,
        a.AttrValue,
        a.Flag
    FROM mp_portal.attr_value AS a
    JOIN mp_portal.industry AS i
    ON a.IndustryID = i.IndustryID
    WHERE a.IsCalc='y'
    AND i.DBName ='{0}';
    """
    return pandas.read_sql_query(sql.format(db), get_mysql_connect(env="mpportal"))


def get_categories(db=u"mp_women_clothing", category_id_list=[121412004]):
    sql = """
    SELECT DISTINCT
        c.CategoryID,
        c.CategoryName,
        c.CategoryDesc
    FROM mp_portal.category AS c
    JOIN mp_portal.industry AS i
    ON c.IndustryID = i.IndustryID
    WHERE i.DBName = '{0}' {1}
    AND c.IsBottom = 'y';
    """
    if category_id_list:
        category_filter = u"AND CategoryID IN ("
        for category_id in category_id_list:
            category_filter += u"{0},".format(category_id)
        category_filter = category_filter[0:-1] + u")"
    else:
        category_filter = u""
    return pandas.read_sql_query(sql.format(db, category_filter), get_mysql_connect(env="mpportal")).values


if __name__ == "__main__":
    _industry = "mp_women_clothing"
    _source_table = "itemmonthlysales2015"
    _target_table = "itemmonthlyrelation_2015"
    _shop_id = 66098091
    _date_range = "2015-12-01"
    _category_id = 1623

