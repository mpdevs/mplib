# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.competition import Hound
from mplib.IO import Hive


def preprocess(**kwargs):
    sql = """
    USE t_elengjing;
    ADD FILE /home/udflib/serverudf/elengjing/{udf};
    DROP TABLE IF EXISTS competitive_item_train_stage_1;
    CREATE TABLE competitive_item_train_stage_1(
        customer_item_id STRING,
        target_item_id STRING,
        similarity STRING,
        score STRING,
        category_id STRING
    ) STORED AS ORC;
    INSERT INTO competitive_item_train_stage_1
    SELECT TRANSFORM(ct.data, tt.data, ct.itemid, tt.itemid, l.score, l.categoryid)
    USING 'python {udf} {category_id}' AS (customer_item_id, target_item_id, similarity, score, category_id)
    FROM competitive_manual_label AS l
    JOIN elengjing.women_clothing_item_attr_t AS ct
    ON ct.itemid = l.sourceitemid
    JOIN elengjing.women_clothing_item_attr_t AS tt
    ON tt.itemid = l.targetitemid;

    DROP TABLE IF EXISTS competitive_item_train_stage_2;
    CREATE TABLE competitive_item_train_stage_2 AS
    SELECT
        a.customer_item_id,
        a.target_item_id,
        a.category_id,
        cv.tag AS customer_item_dummy,
        tv.tag AS target_item_dummy,
        cn.attrvalue AS customer_item_attr,
        tn.attrvalue AS target_item_attr,
        a.score
    FROM competitive_item_train_stage_1 AS a
    JOIN mpintranet.attrname_export AS cn
    ON a.customer_item_id = cn.itemid
    JOIN mpintranet.attrname_export AS tn
    ON a.target_item_id = tn.itemid
    JOIN mpintranet.attrvalue_export AS cv
    ON a.customer_item_id = cv.itemid
    JOIN mpintranet.attrvalue_export AS tv
    ON a.target_item_id = tv.itemid
    WHERE a.category_id = {category_id};
    """.format(**kwargs)
    print(sql)
    Hive(env="idc").execute(sql)


def train(category_id):
    h = Hound()

    return category_id


if __name__ == "__main__":
    query_dict = dict(
        category_id=1623,
        udf="udf_pair_similarity1.py"
    )
    preprocess(**query_dict)
