USE elengjing_price;
DROP TABLE IF EXISTS elengjing_price.attr_value_text;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_value_text(
    categoryid STRING,
    attr STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY'\t';
LOAD DATA LOCAL INPATH '/home/data/attr_value.csv' OVERWRITE INTO TABLE elengjing_price.attr_value_text;


DROP TABLE IF EXISTS elengjing_price.attr_value;
CREATE TABLE elengjing_price.attr_value
STORED AS ORC
AS
SELECT *
FROM elengjing_price.attr_value_text;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162103;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162103(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


INSERT INTO TABLE elengjing_price.attr_unique_itemid_162103
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162103
AND attrname != '品牌' GROUP BY itemid;


DROP TABLE IF EXISTS elengjing_price.item_attr_162103;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162103(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


INSERT INTO TABLE elengjing_price.item_attr_162103
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162103
AND attrname != '品牌';


DROP TABLE IF EXISTS elengjing_price.item_attr_1_162103;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162103(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


INSERT INTO TABLE elengjing_price.item_attr_1_162103
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162103
GROUP BY
    itemid,
    attr;


DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162103;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162103(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


INSERT INTO TABLE elengjing_price.attr_itemid_value_162103
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162103 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162103;


DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162103;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162103(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


SET mapred.reduce.tasks = 1000;


INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162103
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162103 AS t1
LEFT JOIN elengjing_price.item_attr_1_162103 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;


DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162103;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162103(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162103
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162103
) AS a
GROUP BY itemid;


DROP TABLE attr_itemid_value_162103;
DROP TABLE attr_itemid_value_data_162103;
DROP TABLE attr_unique_itemid_162103;
DROP TABLE item_attr_1_162103;
DROP TABLE item_attr_162103;

DROP TABLE IF EXISTS train_162103_daily;
CREATE TABLE train_162103_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162103_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162103
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162103
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162103 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162104;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162104(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;


INSERT INTO TABLE elengjing_price.attr_unique_itemid_162104
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162104
AND attrname != '品牌' GROUP BY itemid;


DROP TABLE IF EXISTS elengjing_price.item_attr_162104;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162104(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162104
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162104
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162104;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162104(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162104
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162104
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162104;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162104(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162104
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162104 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162104;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162104;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162104(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162104
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162104 AS t1
LEFT JOIN elengjing_price.item_attr_1_162104 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162104;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162104(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162104
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162104
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162104;
    DROP TABLE attr_itemid_value_data_162104;
    DROP TABLE attr_unique_itemid_162104;
    DROP TABLE item_attr_1_162104;
    DROP TABLE item_attr_162104;


USE elengjing_price;
DROP TABLE IF EXISTS train_162104_daily;
CREATE TABLE train_162104_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162104_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162104
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162104
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162104 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162201;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162201(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162201
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162201
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162201;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162201(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162201
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162201
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162201;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162201(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162201
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162201
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162201;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162201(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162201
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162201 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162201;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162201;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162201(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162201
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162201 AS t1
LEFT JOIN elengjing_price.item_attr_1_162201 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162201;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162201(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162201
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162201
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162201;
    DROP TABLE attr_itemid_value_data_162201;
    DROP TABLE attr_unique_itemid_162201;
    DROP TABLE item_attr_1_162201;
    DROP TABLE item_attr_162201;


USE elengjing_price;
DROP TABLE IF EXISTS train_162201_daily;
CREATE TABLE train_162201_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162201_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162201
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162201
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162201 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162205;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162205(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162205
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162205
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162205;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162205(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162205
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162205
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162205;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162205(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162205
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162205
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162205;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162205(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162205
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162205 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162205;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162205;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162205(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162205
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162205 AS t1
LEFT JOIN elengjing_price.item_attr_1_162205 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162205;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162205(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162205
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162205
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162205;
    DROP TABLE attr_itemid_value_data_162205;
    DROP TABLE attr_unique_itemid_162205;
    DROP TABLE item_attr_1_162205;
    DROP TABLE item_attr_162205;


USE elengjing_price;
DROP TABLE IF EXISTS train_162205_daily;
CREATE TABLE train_162205_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162205_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162205
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162205
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162205 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162401;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162401(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162401
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162401
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162401;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162401(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162401
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162401
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162401;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162401(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162401
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162401
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162401;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162401(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162401
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162401 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162401;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162401;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162401(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162401
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162401 AS t1
LEFT JOIN elengjing_price.item_attr_1_162401 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162401;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162401(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162401
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162401
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162401;
    DROP TABLE attr_itemid_value_data_162401;
    DROP TABLE attr_unique_itemid_162401;
    DROP TABLE item_attr_1_162401;
    DROP TABLE item_attr_162401;


DROP TABLE IF EXISTS train_162401_daily;
CREATE TABLE train_162401_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162401_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162401
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162401
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162401 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162402;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162402(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162402
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162402
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162402;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162402(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162402
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162402
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162402;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162402(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162402
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162402
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162402;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162402(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162402
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162402 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162402;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162402;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162402(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162402
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162402 AS t1
LEFT JOIN elengjing_price.item_attr_1_162402 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162402;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162402(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162402
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162402
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162402;
    DROP TABLE attr_itemid_value_data_162402;
    DROP TABLE attr_unique_itemid_162402;
    DROP TABLE item_attr_1_162402;
    DROP TABLE item_attr_162402;


USE elengjing_price;
DROP TABLE IF EXISTS train_162402_daily;
CREATE TABLE train_162402_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162402_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162402
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162402
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162402 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162403;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162403(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162403
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162403
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162403;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162403(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162403
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162403
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162403;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162403(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162403
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162403
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162403;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162403(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162403
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162403 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162403;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162403;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162403(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162403
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162403 AS t1
LEFT JOIN elengjing_price.item_attr_1_162403 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162403;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162403(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162403
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162403
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162403;
    DROP TABLE attr_itemid_value_data_162403;
    DROP TABLE attr_unique_itemid_162403;
    DROP TABLE item_attr_1_162403;
    DROP TABLE item_attr_162403;


USE elengjing_price;
DROP TABLE IF EXISTS train_162403_daily;
CREATE TABLE train_162403_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162403_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162403
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162403
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162403 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162404;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162404(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162404
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162404
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162404;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162404(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162404
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162404
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162404;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162404(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162404
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162404
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162404;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162404(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162404
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162404 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162404;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162404;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162404(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162404
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162404 AS t1
LEFT JOIN elengjing_price.item_attr_1_162404 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162404;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162404(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162404
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162404
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162404;
    DROP TABLE attr_itemid_value_data_162404;
    DROP TABLE attr_unique_itemid_162404;
    DROP TABLE item_attr_1_162404;
    DROP TABLE item_attr_162404;


USE elengjing_price;
DROP TABLE IF EXISTS train_162404_daily;
CREATE TABLE train_162404_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162404_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162404
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162404
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162404 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162701;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162701(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162701
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162701
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162701;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162701(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162701
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162701
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162701;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162701(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162701
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162701
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162701;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162701(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162701
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162701 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162701;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162701;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162701(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162701
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162701 AS t1
LEFT JOIN elengjing_price.item_attr_1_162701 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162701;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162701(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162701
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162701
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162701;
    DROP TABLE attr_itemid_value_data_162701;
    DROP TABLE attr_unique_itemid_162701;
    DROP TABLE item_attr_1_162701;
    DROP TABLE item_attr_162701;


USE elengjing_price;
DROP TABLE IF EXISTS train_162701_daily;
CREATE TABLE train_162701_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162701_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162701
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162701
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162701 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162702;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162702(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162702
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162702
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162702;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162702(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162702
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162702
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162702;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162702(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162702
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162702
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162702;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162702(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162702
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162702 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162702;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162702;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162702(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162702
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162702 AS t1
LEFT JOIN elengjing_price.item_attr_1_162702 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162702;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162702(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162702
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162702
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162702;
    DROP TABLE attr_itemid_value_data_162702;
    DROP TABLE attr_unique_itemid_162702;
    DROP TABLE item_attr_1_162702;
    DROP TABLE item_attr_162702;


USE elengjing_price;
DROP TABLE IF EXISTS train_162702_daily;
CREATE TABLE train_162702_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162702_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162702
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162702
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162702 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162703;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162703(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162703
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162703
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162703;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162703(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162703
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162703
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162703;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162703(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162703
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162703
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162703;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162703(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162703
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162703 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162703;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162703;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162703(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162703
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162703 AS t1
LEFT JOIN elengjing_price.item_attr_1_162703 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162703;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162703(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162703
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162703
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162703;
    DROP TABLE attr_itemid_value_data_162703;
    DROP TABLE attr_unique_itemid_162703;
    DROP TABLE item_attr_1_162703;
    DROP TABLE item_attr_162703;


USE elengjing_price;
DROP TABLE IF EXISTS train_162703_daily;
CREATE TABLE train_162703_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162703_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162703
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162703
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162703 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50000671;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50000671(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50000671
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50000671
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50000671;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50000671(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50000671
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50000671
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50000671;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50000671(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50000671
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50000671
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50000671;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50000671(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50000671
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50000671 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50000671;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50000671;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50000671(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50000671
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50000671 AS t1
LEFT JOIN elengjing_price.item_attr_1_50000671 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50000671;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50000671(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50000671
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50000671
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50000671;
    DROP TABLE attr_itemid_value_data_50000671;
    DROP TABLE attr_unique_itemid_50000671;
    DROP TABLE item_attr_1_50000671;
    DROP TABLE item_attr_50000671;


USE elengjing_price;
DROP TABLE IF EXISTS train_50000671_daily;
CREATE TABLE train_50000671_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50000671_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50000671
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50000671
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50000671 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50000697;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50000697(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50000697
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50000697
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50000697;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50000697(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50000697
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50000697
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50000697;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50000697(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50000697
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50000697
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50000697;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50000697(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50000697
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50000697 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50000697;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50000697;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50000697(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50000697
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50000697 AS t1
LEFT JOIN elengjing_price.item_attr_1_50000697 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50000697;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50000697(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50000697
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50000697
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50000697;
    DROP TABLE attr_itemid_value_data_50000697;
    DROP TABLE attr_unique_itemid_50000697;
    DROP TABLE item_attr_1_50000697;
    DROP TABLE item_attr_50000697;


USE elengjing_price;
DROP TABLE IF EXISTS train_50000697_daily;
CREATE TABLE train_50000697_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50000697_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50000697
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50000697
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50000697 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50000852;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50000852(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50000852
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50000852
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50000852;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50000852(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50000852
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50000852
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50000852;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50000852(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50000852
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50000852
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50000852;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50000852(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50000852
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50000852 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50000852;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50000852;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50000852(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50000852
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50000852 AS t1
LEFT JOIN elengjing_price.item_attr_1_50000852 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50000852;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50000852(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50000852
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50000852
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50000852;
    DROP TABLE attr_itemid_value_data_50000852;
    DROP TABLE attr_unique_itemid_50000852;
    DROP TABLE item_attr_1_50000852;
    DROP TABLE item_attr_50000852;


USE elengjing_price;
DROP TABLE IF EXISTS train_50000852_daily;
CREATE TABLE train_50000852_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50000852_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50000852
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50000852
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50000852 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50003509;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50003509(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50003509
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50003509
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50003509;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50003509(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50003509
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50003509
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50003509;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50003509(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50003509
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50003509
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50003509;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50003509(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50003509
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50003509 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50003509;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50003509;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50003509(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50003509
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50003509 AS t1
LEFT JOIN elengjing_price.item_attr_1_50003509 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50003509;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50003509(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50003509
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50003509
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50003509;
    DROP TABLE attr_itemid_value_data_50003509;
    DROP TABLE attr_unique_itemid_50003509;
    DROP TABLE item_attr_1_50003509;
    DROP TABLE item_attr_50003509;


USE elengjing_price;
DROP TABLE IF EXISTS train_50003509_daily;
CREATE TABLE train_50003509_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50003509_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50003509
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50003509
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50003509 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50003511;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50003511(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50003511
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50003511
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50003511;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50003511(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50003511
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50003511
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50003511;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50003511(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50003511
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50003511
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50003511;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50003511(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50003511
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50003511 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50003511;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50003511;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50003511(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50003511
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50003511 AS t1
LEFT JOIN elengjing_price.item_attr_1_50003511 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50003511;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50003511(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50003511
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50003511
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50003511;
    DROP TABLE attr_itemid_value_data_50003511;
    DROP TABLE attr_unique_itemid_50003511;
    DROP TABLE item_attr_1_50003511;
    DROP TABLE item_attr_50003511;


USE elengjing_price;
DROP TABLE IF EXISTS train_50003511_daily;
CREATE TABLE train_50003511_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50003511_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50003511
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50003511
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50003511 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50005065;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50005065(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50005065
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50005065
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50005065;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50005065(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50005065
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50005065
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50005065;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50005065(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50005065
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50005065
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50005065;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50005065(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50005065
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50005065 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50005065;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50005065;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50005065(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50005065
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50005065 AS t1
LEFT JOIN elengjing_price.item_attr_1_50005065 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50005065;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50005065(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50005065
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50005065
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50005065;
    DROP TABLE attr_itemid_value_data_50005065;
    DROP TABLE attr_unique_itemid_50005065;
    DROP TABLE item_attr_1_50005065;
    DROP TABLE item_attr_50005065;


USE elengjing_price;
DROP TABLE IF EXISTS train_50005065_daily;
CREATE TABLE train_50005065_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50005065_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50005065
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50005065
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50005065 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_1623;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_1623(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_1623
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 1623
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_1623;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1623(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1623
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 1623
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_1623;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_1623(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_1623
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_1623
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_1623;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_1623(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_1623
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_1623 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 1623;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_1623;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_1623(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_1623
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_1623 AS t1
LEFT JOIN elengjing_price.item_attr_1_1623 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_1623;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_1623(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_1623
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_1623
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_1623;
    DROP TABLE attr_itemid_value_data_1623;
    DROP TABLE attr_unique_itemid_1623;
    DROP TABLE item_attr_1_1623;
    DROP TABLE item_attr_1623;


USE elengjing_price;
DROP TABLE IF EXISTS train_1623_daily;
CREATE TABLE train_1623_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_1623_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 1623
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 1623
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_1623 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_1629;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_1629(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_1629
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 1629
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_1629;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1629(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1629
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 1629
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_1629;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_1629(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_1629
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_1629
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_1629;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_1629(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_1629
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_1629 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 1629;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_1629;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_1629(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_1629
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_1629 AS t1
LEFT JOIN elengjing_price.item_attr_1_1629 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_1629;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_1629(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_1629
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_1629
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_1629;
    DROP TABLE attr_itemid_value_data_1629;
    DROP TABLE attr_unique_itemid_1629;
    DROP TABLE item_attr_1_1629;
    DROP TABLE item_attr_1629;


USE elengjing_price;
DROP TABLE IF EXISTS train_1629_daily;
CREATE TABLE train_1629_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_1629_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 1629
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 1629
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_1629 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_162116;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_162116(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_162116
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162116
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_162116;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_162116(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_162116
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 162116
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_162116;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_162116(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_162116
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_162116
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_162116;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_162116(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_162116
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_162116 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 162116;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_162116;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_162116(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_162116
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_162116 AS t1
LEFT JOIN elengjing_price.item_attr_1_162116 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_162116;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_162116(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_162116
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_162116
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_162116;
    DROP TABLE attr_itemid_value_data_162116;
    DROP TABLE attr_unique_itemid_162116;
    DROP TABLE item_attr_1_162116;
    DROP TABLE item_attr_162116;


USE elengjing_price;
DROP TABLE IF EXISTS train_162116_daily;
CREATE TABLE train_162116_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_162116_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162116
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 162116
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_162116 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50003510;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50003510(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50003510
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50003510
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50003510;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50003510(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50003510
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50003510
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50003510;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50003510(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50003510
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50003510
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50003510;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50003510(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50003510
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50003510 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50003510;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50003510;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50003510(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50003510
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50003510 AS t1
LEFT JOIN elengjing_price.item_attr_1_50003510 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50003510;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50003510(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50003510
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50003510
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50003510;
    DROP TABLE attr_itemid_value_data_50003510;
    DROP TABLE attr_unique_itemid_50003510;
    DROP TABLE item_attr_1_50003510;
    DROP TABLE item_attr_50003510;


USE elengjing_price;
DROP TABLE IF EXISTS train_50003510_daily;
CREATE TABLE train_50003510_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50003510_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50003510
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50003510
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50003510 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008900;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008900(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008900
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008900
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008900;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008900(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008900
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008900
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008900;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008900(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008900
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008900
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008900;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008900(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008900
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008900 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008900;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008900;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008900(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008900
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008900 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008900 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008900;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008900(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008900
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008900
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008900;
    DROP TABLE attr_itemid_value_data_50008900;
    DROP TABLE attr_unique_itemid_50008900;
    DROP TABLE item_attr_1_50008900;
    DROP TABLE item_attr_50008900;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008900_daily;
CREATE TABLE train_50008900_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008900_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008900
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008900
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008900 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008901;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008901(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008901
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008901
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008901;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008901(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008901
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008901
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008901;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008901(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008901
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008901
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008901;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008901(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008901
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008901 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008901;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008901;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008901(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008901
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008901 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008901 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008901;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008901(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008901
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008901
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008901;
    DROP TABLE attr_itemid_value_data_50008901;
    DROP TABLE attr_unique_itemid_50008901;
    DROP TABLE item_attr_1_50008901;
    DROP TABLE item_attr_50008901;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008901_daily;
CREATE TABLE train_50008901_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008901_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008901
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008901
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008901 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008903;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008903(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008903
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008903
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008903;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008903(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008903
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008903
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008903;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008903(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008903
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008903
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008903;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008903(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008903
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008903 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008903;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008903;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008903(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008903
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008903 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008903 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008903;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008903(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008903
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008903
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008903;
    DROP TABLE attr_itemid_value_data_50008903;
    DROP TABLE attr_unique_itemid_50008903;
    DROP TABLE item_attr_1_50008903;
    DROP TABLE item_attr_50008903;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008903_daily;
CREATE TABLE train_50008903_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008903_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008903
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008903
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008903 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008904;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008904(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008904
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008904
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008904;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008904(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008904
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008904
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008904;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008904(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008904
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008904
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008904;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008904(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008904
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008904 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008904;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008904;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008904(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008904
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008904 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008904 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008904;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008904(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008904
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008904
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008904;
    DROP TABLE attr_itemid_value_data_50008904;
    DROP TABLE attr_unique_itemid_50008904;
    DROP TABLE item_attr_1_50008904;
    DROP TABLE item_attr_50008904;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008904_daily;
CREATE TABLE train_50008904_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008904_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008904
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008904
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008904 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008905;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008905(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008905
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008905
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008905;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008905(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008905
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008905
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008905;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008905(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008905
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008905
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008905;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008905(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008905
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008905 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008905;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008905;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008905(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008905
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008905 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008905 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008905;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008905(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008905
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008905
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008905;
    DROP TABLE attr_itemid_value_data_50008905;
    DROP TABLE attr_unique_itemid_50008905;
    DROP TABLE item_attr_1_50008905;
    DROP TABLE item_attr_50008905;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008905_daily;
CREATE TABLE train_50008905_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008905_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008905
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008905
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008905 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008897;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008897(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008897
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008897
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008897;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008897(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008897
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008897
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008897;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008897(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008897
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008897
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008897;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008897(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008897
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008897 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008897;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008897;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008897(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008897
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008897 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008897 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008897;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008897(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008897
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008897
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008897;
    DROP TABLE attr_itemid_value_data_50008897;
    DROP TABLE attr_unique_itemid_50008897;
    DROP TABLE item_attr_1_50008897;
    DROP TABLE item_attr_50008897;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008897_daily;
CREATE TABLE train_50008897_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008897_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008897
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008897
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008897 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008899;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008899(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008899
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008899
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008899;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008899(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008899
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008899
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008899;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008899(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008899
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008899
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008899;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008899(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008899
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008899 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008899;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008899;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008899(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008899
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008899 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008899 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008899;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008899(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008899
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008899
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008899;
    DROP TABLE attr_itemid_value_data_50008899;
    DROP TABLE attr_unique_itemid_50008899;
    DROP TABLE item_attr_1_50008899;
    DROP TABLE item_attr_50008899;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008899_daily;
CREATE TABLE train_50008899_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008899_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008899
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008899
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008899 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50008898;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50008898(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50008898
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008898
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50008898;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50008898(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50008898
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50008898
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50008898;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50008898(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50008898
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50008898
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50008898;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50008898(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50008898
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50008898 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50008898;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50008898;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50008898(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50008898
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50008898 AS t1
LEFT JOIN elengjing_price.item_attr_1_50008898 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50008898;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50008898(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50008898
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50008898
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50008898;
    DROP TABLE attr_itemid_value_data_50008898;
    DROP TABLE attr_unique_itemid_50008898;
    DROP TABLE item_attr_1_50008898;
    DROP TABLE item_attr_50008898;


USE elengjing_price;
DROP TABLE IF EXISTS train_50008898_daily;
CREATE TABLE train_50008898_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50008898_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008898
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50008898
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50008898 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50011412;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50011412(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50011412
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011412
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50011412;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50011412(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50011412
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011412
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50011412;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50011412(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50011412
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50011412
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50011412;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50011412(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50011412
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50011412 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50011412;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50011412;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50011412(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50011412
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50011412 AS t1
LEFT JOIN elengjing_price.item_attr_1_50011412 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50011412;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50011412(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50011412
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50011412
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50011412;
    DROP TABLE attr_itemid_value_data_50011412;
    DROP TABLE attr_unique_itemid_50011412;
    DROP TABLE item_attr_1_50011412;
    DROP TABLE item_attr_50011412;


USE elengjing_price;
DROP TABLE IF EXISTS train_50011412_daily;
CREATE TABLE train_50011412_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50011412_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011412
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011412
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50011412 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50011277;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50011277(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50011277
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011277
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50011277;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50011277(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50011277
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011277
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50011277;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50011277(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50011277
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50011277
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50011277;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50011277(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50011277
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50011277 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50011277;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50011277;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50011277(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50011277
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50011277 AS t1
LEFT JOIN elengjing_price.item_attr_1_50011277 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50011277;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50011277(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50011277
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50011277
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50011277;
    DROP TABLE attr_itemid_value_data_50011277;
    DROP TABLE attr_unique_itemid_50011277;
    DROP TABLE item_attr_1_50011277;
    DROP TABLE item_attr_50011277;


USE elengjing_price;
DROP TABLE IF EXISTS train_50011277_daily;
CREATE TABLE train_50011277_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50011277_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011277
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011277
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50011277 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50011411;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50011411(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50011411
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011411
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50011411;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50011411(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50011411
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011411
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50011411;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50011411(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50011411
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50011411
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50011411;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50011411(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50011411
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50011411 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50011411;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50011411;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50011411(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50011411
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50011411 AS t1
LEFT JOIN elengjing_price.item_attr_1_50011411 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50011411;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50011411(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50011411
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50011411
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50011411;
    DROP TABLE attr_itemid_value_data_50011411;
    DROP TABLE attr_unique_itemid_50011411;
    DROP TABLE item_attr_1_50011411;
    DROP TABLE item_attr_50011411;


USE elengjing_price;
DROP TABLE IF EXISTS train_50011411_daily;
CREATE TABLE train_50011411_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50011411_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011411
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011411
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50011411 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50011413;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50011413(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50011413
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011413
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50011413;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50011413(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50011413
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50011413
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50011413;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50011413(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50011413
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50011413
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50011413;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50011413(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50011413
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50011413 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50011413;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50011413;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50011413(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50011413
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50011413 AS t1
LEFT JOIN elengjing_price.item_attr_1_50011413 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50011413;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50011413(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50011413
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50011413
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50011413;
    DROP TABLE attr_itemid_value_data_50011413;
    DROP TABLE attr_unique_itemid_50011413;
    DROP TABLE item_attr_1_50011413;
    DROP TABLE item_attr_50011413;


USE elengjing_price;
DROP TABLE IF EXISTS train_50011413_daily;
CREATE TABLE train_50011413_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50011413_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011413
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50011413
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50011413 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50013196;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50013196(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50013196
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50013196
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50013196;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50013196(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50013196
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50013196
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50013196;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50013196(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50013196
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50013196
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50013196;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50013196(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50013196
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50013196 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50013196;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50013196;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50013196(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50013196
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50013196 AS t1
LEFT JOIN elengjing_price.item_attr_1_50013196 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50013196;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50013196(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50013196
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50013196
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50013196;
    DROP TABLE attr_itemid_value_data_50013196;
    DROP TABLE attr_unique_itemid_50013196;
    DROP TABLE item_attr_1_50013196;
    DROP TABLE item_attr_50013196;


USE elengjing_price;
DROP TABLE IF EXISTS train_50013196_daily;
CREATE TABLE train_50013196_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50013196_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50013196
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50013196
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50013196 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50026651;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50026651(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50026651
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50026651
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50026651;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50026651(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50026651
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50026651
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50026651;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50026651(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50026651
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50026651
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50026651;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50026651(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50026651
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50026651 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50026651;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50026651;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50026651(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50026651
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50026651 AS t1
LEFT JOIN elengjing_price.item_attr_1_50026651 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50026651;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50026651(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50026651
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50026651
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50026651;
    DROP TABLE attr_itemid_value_data_50026651;
    DROP TABLE attr_unique_itemid_50026651;
    DROP TABLE item_attr_1_50026651;
    DROP TABLE item_attr_50026651;


USE elengjing_price;
DROP TABLE IF EXISTS train_50026651_daily;
CREATE TABLE train_50026651_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50026651_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50026651
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50026651
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50026651 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_121434004;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_121434004(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_121434004
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 121434004
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_121434004;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_121434004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_121434004
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 121434004
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_121434004;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_121434004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_121434004
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_121434004
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_121434004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_121434004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_121434004
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_121434004 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 121434004;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_121434004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_121434004(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_121434004
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_121434004 AS t1
LEFT JOIN elengjing_price.item_attr_1_121434004 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_121434004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_121434004(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_121434004
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_121434004
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_121434004;
    DROP TABLE attr_itemid_value_data_121434004;
    DROP TABLE attr_unique_itemid_121434004;
    DROP TABLE item_attr_1_121434004;
    DROP TABLE item_attr_121434004;


USE elengjing_price;
DROP TABLE IF EXISTS train_121434004_daily;
CREATE TABLE train_121434004_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_121434004_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 121434004
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 121434004
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_121434004 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_123216004;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_123216004(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_123216004
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 123216004
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_123216004;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_123216004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_123216004
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 123216004
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_123216004;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_123216004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_123216004
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_123216004
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_123216004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_123216004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_123216004
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_123216004 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 123216004;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_123216004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_123216004(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_123216004
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_123216004 AS t1
LEFT JOIN elengjing_price.item_attr_1_123216004 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_123216004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_123216004(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_123216004
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_123216004
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_123216004;
    DROP TABLE attr_itemid_value_data_123216004;
    DROP TABLE attr_unique_itemid_123216004;
    DROP TABLE item_attr_1_123216004;
    DROP TABLE item_attr_123216004;


USE elengjing_price;
DROP TABLE IF EXISTS train_123216004_daily;
CREATE TABLE train_123216004_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_123216004_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 123216004
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 123216004
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_123216004 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50007068;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50007068(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50007068
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50007068
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50007068;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50007068(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50007068
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50007068
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50007068;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50007068(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50007068
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50007068
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50007068;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50007068(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50007068
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50007068 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50007068;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50007068;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50007068(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50007068
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50007068 AS t1
LEFT JOIN elengjing_price.item_attr_1_50007068 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50007068;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50007068(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50007068
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50007068
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50007068;
    DROP TABLE attr_itemid_value_data_50007068;
    DROP TABLE attr_unique_itemid_50007068;
    DROP TABLE item_attr_1_50007068;
    DROP TABLE item_attr_50007068;


USE elengjing_price;
DROP TABLE IF EXISTS train_50007068_daily;
CREATE TABLE train_50007068_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50007068_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50007068
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50007068
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50007068 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50010850;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50010850(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50010850
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50010850
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50010850;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50010850(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50010850
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50010850
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50010850;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50010850(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50010850
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50010850
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50010850;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50010850(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50010850
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50010850 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50010850;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50010850;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50010850(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50010850
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50010850 AS t1
LEFT JOIN elengjing_price.item_attr_1_50010850 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50010850;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50010850(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50010850
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50010850
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50010850;
    DROP TABLE attr_itemid_value_data_50010850;
    DROP TABLE attr_unique_itemid_50010850;
    DROP TABLE item_attr_1_50010850;
    DROP TABLE item_attr_50010850;


USE elengjing_price;
DROP TABLE IF EXISTS train_50010850_daily;
CREATE TABLE train_50010850_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50010850_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50010850
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50010850
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50010850 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50013194;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50013194(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50013194
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50013194
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50013194;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50013194(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50013194
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50013194
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50013194;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50013194(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50013194
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50013194
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50013194;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50013194(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50013194
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50013194 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50013194;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50013194;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50013194(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50013194
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50013194 AS t1
LEFT JOIN elengjing_price.item_attr_1_50013194 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50013194;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50013194(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50013194
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50013194
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50013194;
    DROP TABLE attr_itemid_value_data_50013194;
    DROP TABLE attr_unique_itemid_50013194;
    DROP TABLE item_attr_1_50013194;
    DROP TABLE item_attr_50013194;


USE elengjing_price;
DROP TABLE IF EXISTS train_50013194_daily;
CREATE TABLE train_50013194_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50013194_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50013194
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50013194
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50013194 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_50022566;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_50022566(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_50022566
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50022566
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_50022566;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_50022566(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_50022566
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 50022566
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_50022566;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_50022566(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_50022566
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_50022566
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_50022566;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_50022566(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_50022566
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_50022566 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 50022566;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_50022566;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_50022566(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_50022566
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_50022566 AS t1
LEFT JOIN elengjing_price.item_attr_1_50022566 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_50022566;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_50022566(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_50022566
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_50022566
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_50022566;
    DROP TABLE attr_itemid_value_data_50022566;
    DROP TABLE attr_unique_itemid_50022566;
    DROP TABLE item_attr_1_50022566;
    DROP TABLE item_attr_50022566;


USE elengjing_price;
DROP TABLE IF EXISTS train_50022566_daily;
CREATE TABLE train_50022566_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_50022566_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50022566
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 50022566
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_50022566 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;


DROP TABLE IF EXISTS elengjing_price.attr_unique_itemid_121412004;
CREATE TABLE IF NOT EXISTS elengjing_price.attr_unique_itemid_121412004(
    itemid BIGINT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_unique_itemid_121412004
SELECT itemid
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 121412004
AND attrname != '品牌' GROUP BY itemid;

DROP TABLE IF EXISTS elengjing_price.item_attr_121412004;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_121412004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_121412004
SELECT
    itemid,
    CONCAT(attrname, ":", attrvalue)
FROM elengjing.women_clothing_item_attr_v1
WHERE categoryid = 121412004
AND attrname != '品牌';

DROP TABLE IF EXISTS elengjing_price.item_attr_1_121412004;
CREATE TABLE IF NOT EXISTS elengjing_price.item_attr_1_121412004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.item_attr_1_121412004
SELECT
    itemid,
    attr
FROM elengjing_price.item_attr_121412004
GROUP BY
    itemid,
    attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_121412004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_121412004(
    itemid BIGINT,
    attr STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_121412004
SELECT
    t1.itemid,
    t2.attr
FROM elengjing_price.attr_unique_itemid_121412004 AS t1
CROSS JOIN elengjing_price.attr_value AS t2
WHERE t2.categoryid = 121412004;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_data_121412004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_data_121412004(
    itemid BIGINT,
    attr STRING,
    data INT
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

SET mapred.reduce.tasks = 1000;

INSERT INTO TABLE elengjing_price.attr_itemid_value_data_121412004
SELECT
    t1.*,
    IF(NVL(t2.itemid, 0) = 0, 0, 1)
FROM elengjing_price.attr_itemid_value_121412004 AS t1
LEFT JOIN elengjing_price.item_attr_1_121412004 AS t2
ON t1.itemid = t2.itemid
AND t1.attr = t2.attr;

DROP TABLE IF EXISTS elengjing_price.attr_itemid_value_matrix_121412004;
CREATE TABLE  IF NOT EXISTS elengjing_price.attr_itemid_value_matrix_121412004(
    itemid BIGINT,
    attr STRING,
    data STRING
)
CLUSTERED BY (itemid) INTO 997 BUCKETS
STORED AS ORC;

INSERT INTO TABLE elengjing_price.attr_itemid_value_matrix_121412004
SELECT
    itemid,
    CONCAT_WS(',', COLLECT_LIST(attr)) AS attr,
    CONCAT_WS(',', COLLECT_LIST(CAST(data AS STRING))) AS data
FROM (
    SELECT
        itemid,
        attr,
        data,
        row_number() OVER (PARTITION BY itemid ORDER BY attr DESC) AS rank
    FROM elengjing_price.attr_itemid_value_data_121412004
) AS a
GROUP BY itemid;


    USE elengjing_price;
    DROP TABLE attr_itemid_value_121412004;
    DROP TABLE attr_itemid_value_data_121412004;
    DROP TABLE attr_unique_itemid_121412004;
    DROP TABLE item_attr_1_121412004;
    DROP TABLE item_attr_121412004;


USE elengjing_price;
DROP TABLE IF EXISTS train_121412004_daily;
CREATE TABLE train_121412004_daily (
    itemid STRING,
    data STRING
)CLUSTERED BY (itemid) INTO 113 BUCKETS;

INSERT INTO train_121412004_daily
SELECT
    i.itemid,
    CONCAT_WS(
        ',',
        NVL(CAST(i.avg_price AS STRING), '0'),
        NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
        NVL(CAST(shop.level AS STRING), '0'),
        NVL(CAST(shop.favor AS STRING), '0'),
        NVL(CAST(s.all_spu AS STRING), '0'),
        NVL(CAST(s.all_sales_rank AS STRING), '0'),
        NVL(CAST(sc.category_spu AS STRING), '0'),
        NVL(CAST(sc.category_sales_rank AS STRING), '0'),
        NVL(CAST(s.shop_avg_price AS STRING), '0'),
        NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
        NVL(CAST(sc.category_avg_price AS STRING), '0'),
        NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
        v.data
  ) AS data
FROM
  (SELECT
    shopid, itemid,
    MAX(listeddate) AS listeddate,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 121412004
  GROUP BY
    shopid, itemid) AS i
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS all_spu,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  GROUP BY
    shopid) AS s
ON i.shopid = s.shopid
JOIN
  (SELECT
    shopid,
    COUNT(DISTINCT itemid) AS category_spu,
    ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
    ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
    CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
  FROM elengjing.women_clothing_item
  WHERE daterange BETWEEN '2016-05-01' AND '2016-05-24'
  AND categoryid = 121412004
  GROUP BY
    shopid) AS sc
ON i.shopid = sc.shopid
JOIN elengjing_base.shop AS shop
ON i.shopid = shop.shopid
JOIN elengjing_price.attr_itemid_value_matrix_121412004 AS v
ON i.itemid = v.itemid
WHERE shop.level IS NOT NULL
AND shop.favor IS NOT NULL
AND i.avg_price BETWEEN 10 AND 10000;

