USE t_elengjing;
ADD FILE /home/udflib/serverudf/elengjing/udf_pair_predict2.py;
DROP TABLE IF EXISTS competitive_item_predict;
CREATE TABLE competitive_item_predict(
    customer_item_id BIGINT,
    target_item_id BIGINT,
    category_id BIGINT,
    x STRING,
    y STRING
);
INSERT INTO competitive_item_predict
SELECT TRANSFORM(
    customer_item_id,
    target_item_id,
    category_id,
    x,
    y
)
USING 'python udf_pair_predict2.py 1623' AS (
    customer_item_id,
    target_item_id,
    category_id,
    x,
    y
)
FROM competitive_item_predict_stage_3;
