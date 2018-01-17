USE transforms;
ADD FILE /home/udflib/udf_mother_baby_201711131137.py;
DROP TABLE IF EXISTS post_noise_v2_test;
CREATE TABLE post_noise_v2_test(is_noise STRING, id STRING) STORED AS ORC;
INSERT INTO post_noise_v2_test
SELECT TRANSFORM(content, id) USING 'python udf_mother_baby_201711131137.py' AS (is_noise, id)
FROM transforms.post_noise_v2_base;

SELECT COUNT(*), is_noise FROM post_noise_v2_test GROUP BY is_noise;
