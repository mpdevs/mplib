USE t_elengjing;
ADD FILE /home/udflib/serverudf/elengjing/udf_pair_similarity1.py;
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
USING 'python udf_pair_similarity1.py 1623' AS (customer_item_id, target_item_id, similarity, score, category_id)
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
ON a.target_item_id = tv.itemid;

