USE t_elengjing;
-- train
ADD FILE /home/udflib/serverudf/elengjing/udf_pair_features4.py;
DROP TABLE IF EXISTS competitive_item_train_stage_3;
CREATE TABLE competitive_item_train_stage_3(
    customer_item_id BIGINT,
    target_item_id BIGINT,
    category_id BIGINT,
    x STRING,
    y STRING
) STORED AS ORC;
INSERT INTO competitive_item_train_stage_3
SELECT TRANSFORM(
    customer_item_id,
    target_item_id,
    category_id,
    customer_item_dummy,
    target_item_dummy,
    customer_item_attr,
    target_item_attr,
    score
)
USING 'python udf_pair_features4.py 1623' AS (
    customer_item_id,
    target_item_id,
    category_id,
    x,
    y
)
FROM competitive_item_train_stage_2;

-- predict
DROP TABLE IF EXISTS competitive_item_predict_stage_3;
CREATE TABLE competitive_item_predict_stage_3(
    customer_item_id BIGINT,
    target_item_id BIGINT,
    category_id BIGINT,
    x STRING,
    y STRING
) STORED AS ORC;
INSERT INTO competitive_item_predict_stage_3
SELECT TRANSFORM(
    customer_item_id,
    target_item_id,
    category_id,
    customer_item_dummy,
    target_item_dummy,
    customer_item_attr,
    target_item_attr,
    score
)
USING 'python udf_pair_features4.py 1623' AS (
    customer_item_id,
    target_item_id,
    category_id,
    x,
    y
)
FROM competitive_item_predict_stage_2;
