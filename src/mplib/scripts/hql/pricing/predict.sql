USE elengjing_price;
ADD FILE /home/script/normal_servers/serverudf/elengjing/udf_item_pricing12.py;

DROP TABLE IF EXISTS predict_162103_daily;
CREATE TABLE predict_162103_daily (
    itemid STRING,
    price DECIMAL(20,2)
) STORED AS ORC;
INSERT INTO predict_162103_daily
SELECT TRANSFORM(itemid, data)
USING 'python udf_item_pricing12.py 162103 daily' AS (itemid, price)
FROM (
    SELECT
        i.itemid,
        CONCAT_WS(
            ',',
            CAST(i.avg_price AS STRING),
            CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING),
            CAST(shop.level AS STRING),
            CAST(shop.favor AS STRING),
            CAST(s.all_spu AS STRING),
            CAST(s.all_sales_rank AS STRING),
            CAST(sc.category_spu AS STRING),
            CAST(sc.category_sales_rank AS STRING),
            CAST(s.shop_avg_price AS STRING),
            CAST(s.shop_avg_price_rank AS STRING),
            CAST(sc.category_avg_price AS STRING),
            CAST(sc.category_avg_price_rank AS STRING),
            v.data
        ) AS data
    FROM (
        SELECT
            shopid, itemid,
            MAX(listeddate) AS listeddate,
            CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
        FROM elengjing.women_clothing_item
        WHERE categoryid = 162103
        GROUP BY
            shopid, itemid
    ) AS i
    JOIN (
        SELECT
            shopid,
            COUNT(DISTINCT itemid) AS all_spu,
            CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
            ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
            ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
        FROM elengjing.women_clothing_item
        GROUP BY
            shopid
    ) AS s
    ON i.shopid = s.shopid
    JOIN (
        SELECT
            shopid,
            COUNT(DISTINCT itemid) AS category_spu,
            ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
            ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
            CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
        FROM elengjing.women_clothing_item
        WHERE categoryid = 162103
        GROUP BY
            shopid
        ) AS sc
    ON i.shopid = sc.shopid
    JOIN elengjing_base.shop AS shop
    ON i.shopid = shop.shopid
    JOIN elengjing_price.attr_itemid_value_matrix_162103 AS v
    ON i.itemid = v.itemid
) AS tmp;

