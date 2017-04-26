USE t_elengjing;
DROP TABLE IF EXISTS competitive_filtering_stage_1;
create TABLE competitive_filtering_stage_1 AS 
SELECT 
    d.data AS customer_attr,
    e.data AS target_attr,
    c.customer_item_id,
    c.target_item_id
FROM (
    SELECT a.customer_item_id, b.target_item_id
    FROM (
        SELECT
            itemid AS customer_item_id,
            AVG(discountprice) AS discountprice
        FROM elengjing.women_clothing_item
        WHERE daterange BETWEEN '2017-01-23' AND '2017-01-29'
        AND shopid = 113462750
        AND categoryid = 1623
        GROUP BY customer_item_id
    ) AS a
    CROSS JOIN (
        SELECT
            itemid AS target_item_id,
            AVG(discountprice) AS discountprice
        FROM elengjing.women_clothing_item
        WHERE daterange BETWEEN '2017-01-23' AND '2017-01-29'
        AND shopid != 113462750
        AND categoryid = 1623
        GROUP BY target_item_id
    ) AS b
    WHERE a.discountprice BETWEEN 0.8 * b.discountprice AND 1.2 * b.discountprice
) AS c
JOIN elengjing.women_clothing_item_attr_t AS d
ON c.customer_item_id = d.itemid
JOIN elengjing.women_clothing_item_attr_t AS e
ON c.target_item_id = e.itemid;


ADD FILE /home/udflib/serverudf/elengjing/udf_pair_filter3.py;
DROP TABLE IF EXISTS competitive_filtering_stage_2;
CREATE TABLE competitive_filtering_stage_2(customer_item_id STRING, target_item_id STRING, similarity STRING) STORED AS ORC;
INSERT INTO competitive_filtering_stage_2 
SELECT TRANSFORM(customer_attr, target_attr, customer_item_id, target_item_id) 
USING 'python udf_pair_filter3.py' AS (customer_item_id, target_item_id, similarity)
FROM competitive_filtering_stage_1;
