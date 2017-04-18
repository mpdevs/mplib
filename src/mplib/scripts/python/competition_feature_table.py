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
    drop table if exists mpintranet.attrname;
    CREATE TABLE  if not exists mpintranet.attrname(
    attrname  String
    )
    CLUSTERED BY (attrname) INTO 113 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.attrname
    select attrname from elengjing.women_clothing_item_attr where categoryid = {0} group by attrname
    ;

    drop table if exists mpintranet.itemid;
    CREATE TABLE  if not exists mpintranet.itemid(
    itemid  BIGINT
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.itemid
    select itemid from elengjing.women_clothing_item_attr where categoryid = {0} group by itemid
    ;

    drop table if exists mpintranet.item_attr;
    CREATE TABLE  if not exists mpintranet.item_attr(
    itemid  BIGINT,
    attrname  String,
    attrvalue  String
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    insert into table mpintranet.item_attr
    select
    t.itemid, t.attrname, group_concat(t.attrvalue, ":") as attrvalue
    from
    (
        SELECT
        itemid, attrname, attrvalue
        FROM
        elengjing.women_clothing_item_attr where categoryid = {0}
        DISTRIBUTE BY itemid, attrname
        SORT BY attrvalue desc
    ) t
    group by t.itemid, t.attrname
    ;

    drop table if exists mpintranet.itemid_attrname;
    CREATE TABLE  if not exists mpintranet.itemid_attrname(
    itemid  BIGINT,
    attrname  String
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    insert into table mpintranet.itemid_attrname
    select
    b.itemid,
    a.attrname
    from
    mpintranet.attrname a
    cross join
    mpintranet.itemid b
    ;

    drop table if exists mpintranet.itemid_attrname_attrvalue;
    CREATE TABLE  if not exists mpintranet.itemid_attrname_attrvalue(
    itemid  BIGINT,
    attrname  String,
    attrvalue  String
    )
    CLUSTERED BY (itemid) INTO 783 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.itemid_attrname_attrvalue
    select
    a.itemid,
    a.attrname,
    nvl(b.attrvalue, '')
    from
    mpintranet.itemid_attrname a
    left join
    mpintranet.item_attr b
    on a.itemid = b.itemid and a.attrname = b.attrname
    ;

    drop table if exists mpintranet.attrname_result;
    CREATE TABLE  if not exists mpintranet.attrname_result(
    itemid  BIGINT,
    attrname  String,
    attrvalue  String
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.attrname_result

    select
    t.itemid, group_concat(t.attrname, ",") as attrname, group_concat(t.attrvalue, ",") as attrvalue
    from
    (
    SELECT
    itemid, attrname, attrvalue
    FROM
    mpintranet.itemid_attrname_attrvalue
    DISTRIBUTE BY itemid
    SORT BY attrname desc
    ) t
    group by t.itemid
    ;

    drop table if exists mpintranet.attrname_export;
    CREATE TABLE  if not exists mpintranet.attrname_export(
    itemid  BIGINT,
    attrvalue  String
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.attrname_export
    select itemid, attrvalue from mpintranet.attrname_result
    ;
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
    drop table if exists mpintranet.attrvalue;
    CREATE TABLE  if not exists mpintranet.attrvalue(
    attrvalue  String
    )
    CLUSTERED BY (attrvalue) INTO 113 BUCKETS
    STORED AS ORC
    ;
    insert into table mpintranet.attrvalue
    select concat(attrname, '_', attrvalue) from elengjing.women_clothing_item_attr where categoryid = {0} group by concat(attrname, '_', attrvalue)
    ;
    drop table if exists mpintranet.itemid;
    CREATE TABLE  if not exists mpintranet.itemid(
    itemid  BIGINT
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.itemid
    select itemid from elengjing.women_clothing_item_attr where categoryid = {0} group by itemid
    ;
    drop table if exists mpintranet.item_attr;
    CREATE TABLE  if not exists mpintranet.item_attr(
    itemid  BIGINT,
    attrvalue  String
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.item_attr
    select
    itemid, concat(attrname, '_', attrvalue) as attrvalue
    from
    elengjing.women_clothing_item_attr
    where categoryid = {0}
    ;
    ------- 产生全量属性表
    drop table if exists mpintranet.itemid_attrvalue;
    CREATE TABLE  if not exists mpintranet.itemid_attrvalue(
    itemid  BIGINT,
    attrvalue  String
    )
    CLUSTERED BY (itemid) INTO 581 BUCKETS
    STORED AS ORC;

    insert into table mpintranet.itemid_attrvalue
    select
    b.itemid,
    a.attrvalue
    from
    mpintranet.attrvalue a
    cross join
    mpintranet.itemid b
    ;
    ---------join 全量表 ，产出结果
    drop table if exists mpintranet.itemid_attrvalue_tag;
    CREATE TABLE  if not exists mpintranet.itemid_attrvalue_tag(
    itemid  BIGINT,
    attrvalue  String,
    tag  String
    )
    CLUSTERED BY (itemid) INTO 783 BUCKETS
    STORED AS ORC
    ;

    set mapred.reduce.tasks=500;
    insert into table mpintranet.itemid_attrvalue_tag
    select
    a.itemid,
    a.attrvalue,
    if(b.itemid is null, '0', '1')
    from
    mpintranet.itemid_attrvalue a
    left join
    mpintranet.item_attr b
    on a.itemid = b.itemid and a.attrvalue = b.attrvalue
    ;
    ---------- join 结果表
    drop table if exists mpintranet.attrvalue_result;
    CREATE TABLE  if not exists mpintranet.attrvalue_result(
    itemid  BIGINT,
    attrvalue  String,
    tag  String
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC
    ;

    set mapred.reduce.tasks=-1;
    insert into table mpintranet.attrvalue_result

    select
    t.itemid, group_concat(t.attrvalue, ",") as attrvalue,  group_concat(t.tag, ",") as tag
    from
    (
    SELECT
    itemid, attrvalue, tag, split(attrvalue, ',')[0] as attrname
    FROM
    mpintranet.itemid_attrvalue_tag
    DISTRIBUTE BY itemid
    SORT BY attrname desc
    ) t
    group by t.itemid
    ;
    ------------end-------------
    drop table if exists mpintranet.attrvalue_export;
    CREATE TABLE  if not exists mpintranet.attrvalue_export(
    itemid  BIGINT,
    tag  String
    )
    CLUSTERED BY (itemid) INTO 113 BUCKETS
    STORED AS ORC
    ;

    insert into table mpintranet.attrvalue_export
    select itemid, tag from mpintranet.attrvalue_result
    ;
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
