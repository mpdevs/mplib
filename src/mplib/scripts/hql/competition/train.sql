USE t_elengjing;
ADD FILE /home/udflib/serverudf/elengjing/udf_pair_features2.py;
DROP TABLE IF EXISTS competitive_item_train_stage_3;
CREATE TABLE competitive_item_train_stage_3(
    customer_item_id BIGINT,
    target_item_id BIGINT,
    category_id BIGINT,
    x STRING,
    y DECIMAL(18, 4)
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
USING 'python udf_pair_features2.py 1623' AS (
    customer_item_id,
    target_item_id,
    category_id,
    x,
    y
)
FROM competitive_item_train_stage_2;

