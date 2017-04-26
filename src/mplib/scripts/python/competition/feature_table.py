# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.IO import PostgreSQL, Hive


ENV = "idc"


def create_attrname_columns(category_id=50000697):
    sql = """
    DROP TABLE IF EXISTS mpintranet.attrname;
    CREATE TABLE IF NOT EXISTS mpintranet.attrname(attrname STRING)
    CLUSTERED BY (attrname) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.attrname
    SELECT attrname
    FROM elengjing.women_clothing_item_attr
    WHERE categoryid = {0}
    GROUP BY attrname;

    DROP TABLE IF EXISTS mpintranet.itemid;
    CREATE TABLE IF NOT EXISTS mpintranet.itemid(itemid BIGINT)
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.itemid
    SELECT itemid
    FROM elengjing.women_clothing_item_attr
    WHERE categoryid = {0}
    GROUP BY itemid;

    DROP TABLE IF EXISTS mpintranet.item_attr;
    CREATE TABLE IF NOT EXISTS mpintranet.item_attr(
        itemid BIGINT,
        attrname STRING,
        attrvalue STRING
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.item_attr
    SELECT
        t.itemid,
        t.attrname,
        GROUP_CONCAT(t.attrvalue, ":") AS attrvalue
    FROM
    (
        SELECT
            itemid,
            attrname,
            attrvalue
        FROM
        elengjing.women_clothing_item_attr
        WHERE categoryid = {0}
        DISTRIBUTE BY itemid, attrname
        SORT BY attrvalue DESC
    ) AS t
    GROUP BY t.itemid, t.attrname;

    DROP TABLE IF EXISTS mpintranet.itemid_attrname;
    CREATE TABLE IF NOT EXISTS mpintranet.itemid_attrname(
        itemid  BIGINT,
        attrname  STRING
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.itemid_attrname
    SELECT
        b.itemid,
        a.attrname
    FROM mpintranet.attrname a
    CROSS JOIN mpintranet.itemid b;

    DROP TABLE IF EXISTS mpintranet.itemid_attrname_attrvalue;
    CREATE TABLE IF NOT EXISTS mpintranet.itemid_attrname_attrvalue(
        itemid  BIGINT,
        attrname  STRING,
        attrvalue  STRING
    )
    CLUSTERED BY (itemid) INTO 783 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.itemid_attrname_attrvalue
    SELECT
        a.itemid,
        a.attrname,
        NVL(b.attrvalue, '') AS attrvalue
    FROM mpintranet.itemid_attrname a
    LEFT JOIN mpintranet.item_attr b
    ON a.itemid = b.itemid
    AND a.attrname = b.attrname;

    DROP TABLE IF EXISTS mpintranet.attrname_result;
    CREATE TABLE IF NOT EXISTS mpintranet.attrname_result(
        itemid  BIGINT,
        attrname  STRING,
        attrvalue  STRING
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.attrname_result
    SELECT
        t.itemid,
        GROUP_CONCAT(t.attrname, ",") AS attrname,
        GROUP_CONCAT(t.attrvalue, ",") AS attrvalue
    FROM
    (
        SELECT
            itemid,
            attrname,
            attrvalue
        FROM mpintranet.itemid_attrname_attrvalue
        DISTRIBUTE BY itemid
        SORT BY attrname DESC
    ) AS t
    GROUP BY t.itemid;

    DROP TABLE IF EXISTS mpintranet.attrname_export;
    CREATE TABLE IF NOT EXISTS mpintranet.attrname_export(
        itemid  BIGINT,
        attrvalue  STRING
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.attrname_export
    SELECT
        itemid,
        attrvalue
    FROM mpintranet.attrname_result;
    """
    Hive(env=ENV).execute(sql.format(category_id))


def insert_attrname_columns_to_pg():
    ret = Hive(env=ENV).query("SELECT attrname FROM mpintranet.attrname_result LIMIT 1")
    if len(ret) > 0:
        attrnames = ret[0]["attrname"]
        ret = PostgreSQL().query("SELECT * FROM text_value WHERE name = 'attrname_columns'")
        if len(ret) > 0:
            sql = "UPDATE text_value SET value = '{0}' WHERE name = 'attrname_columns'".format(attrnames)
        else:
            sql = "INSERT INTO text_value (name, value) VALUES('attrname_columns', '{0}')".format('', attrnames)
        PostgreSQL.execute(sql)


def create_attrname_attrvalue_columns(category_id):
    sql = """

    -------- 1 产生中间表
    DROP TABLE IF EXISTS mpintranet.attrvalue;
    CREATE TABLE IF NOT EXISTS mpintranet.attrvalue(attrvalue STRING)
    CLUSTERED BY (attrvalue) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.attrvalue
    SELECT CONCAT(attrname, '_', attrvalue) AS attrvalue
    FROM elengjing.women_clothing_item_attr
    WHERE categoryid = {0}
    GROUP BY attrvalue;

    DROP TABLE IF EXISTS mpintranet.itemid;
    CREATE TABLE IF NOT EXISTS mpintranet.itemid(itemid  BIGINT)
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.itemid
    SELECT itemid
    FROM elengjing.women_clothing_item_attr
    WHERE categoryid = {0}
    GROUP BY itemid;

    DROP TABLE IF EXISTS mpintranet.item_attr;
    CREATE TABLE IF NOT EXISTS mpintranet.item_attr(
        itemid BIGINT,
        attrvalue STRING
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.item_attr
    SELECT
        itemid,
        CONCAT(attrname, '_', attrvalue) AS attrvalue
    FROM elengjing.women_clothing_item_attr
    WHERE categoryid = {0};
    ------- 产生全量属性表
    DROP TABLE IF EXISTS mpintranet.itemid_attrvalue;
    CREATE TABLE IF NOT EXISTS mpintranet.itemid_attrvalue(
        itemid  BIGINT,
        attrvalue  STRING
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.itemid_attrvalue
    SELECT
        b.itemid,
        a.attrvalue
    FROM mpintranet.attrvalue AS a
    CROSS JOIN mpintranet.itemid AS b;
    ---------JOIN 全量表 ，产出结果
    DROP TABLE IF EXISTS mpintranet.itemid_attrvalue_tag;
    CREATE TABLE IF NOT EXISTS mpintranet.itemid_attrvalue_tag(
        itemid BIGINT,
        attrvalue STRING,
        tag STRING
    )
    CLUSTERED BY (itemid) INTO 783 BUCKETS
    STORED AS ORC;

    SET mapred.reduce.tasks=500;
    INSERT INTO TABLE mpintranet.itemid_attrvalue_tag
    SELECT
        a.itemid,
        a.attrvalue,
        IF(b.itemid IS NULL, '0', '1') AS tag
    FROM mpintranet.itemid_attrvalue a
    LEFT JOIN mpintranet.item_attr b
    ON a.itemid = b.itemid
    AND a.attrvalue = b.attrvalue;
    ---------- JOIN 结果表
    DROP TABLE IF EXISTS mpintranet.attrvalue_result;
    CREATE TABLE IF NOT EXISTS mpintranet.attrvalue_result(
        itemid BIGINT,
        attrvalue STRING,
        tag STRING
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC;

    SET mapred.reduce.tasks = -1;
    INSERT INTO TABLE mpintranet.attrvalue_result
    SELECT
        t.itemid,
        GROUP_CONCAT(t.attrvalue, ",") AS attrvalue,
        GROUP_CONCAT(t.tag, ",") AS tag
    FROM
    (
        SELECT
            itemid,
            attrvalue,
            tag,
            SPLIT(attrvalue, ',')[0] AS attrname
        FROM mpintranet.itemid_attrvalue_tag
        DISTRIBUTE BY itemid
        SORT BY attrname DESC
    ) AS t
    GROUP BY t.itemid;
    ------------end-------------
    DROP TABLE IF EXISTS mpintranet.attrvalue_export;
    CREATE TABLE IF NOT EXISTS mpintranet.attrvalue_export(
        itemid BIGINT,
        tag STRING
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC;

    INSERT INTO TABLE mpintranet.attrvalue_export
    SELECT
        itemid,
        tag
    FROM mpintranet.attrvalue_result;
    """
    Hive(env=ENV).execute(sql.format(category_id))


def insert_attrname_attrvalue_columns_to_pg():
    ret = Hive(env=ENV).query("SELECT attrvalue FROM mpintranet.attrvalue_result LIMIT 1")
    if len(ret) > 0:
        attrvalues = ret[0]["attrvalue"]
        rows = PostgreSQL().query("SELECT * FROM text_value WHERE name = 'attrname_attrvalue_columns'")
        if len(rows) > 0:
            sql = "UPDATE text_value SET value = '{0}' WHERE name = 'attrname_attrvalue_columns'".format(attrvalues)
        else:
            sql = "INSERT INTO text_value (name, value) VALUES('attrname_attrvalue_columns', '{0}')".format(attrvalues)
        PostgreSQL().execute(sql)


def create_table_item_tagged():
    sql = """
    USE elengjing;
    DROP TABLE IF EXISTS women_clothing_item_attr_t;
    CREATE TABLE women_clothing_item_attr_t AS
    SELECT itemid, CONCAT_WS(',', COLLECT_SET(CONCAT(attrname, ':' ,attrvalue))) AS data
    FROM women_clothing_item_attr
    GROUP BY itemid;
    """
    Hive(env=ENV).execute(sql)


if __name__ == "__main__":
    create_table_item_tagged()
