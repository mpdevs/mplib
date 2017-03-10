USE transforms;
ADD FILE /home/udflib/udf_denoise_weibo_201703081414.py;
DROP TABLE IF EXISTS socialmedia_weibo_noise;
CREATE TABLE socialmedia_weibo_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO socialmedia_weibo_noise
SELECT TRANSFORM(content, id) USING 'python udf_denoise_weibo_201703081414.py' AS (id, is_noise) FROM socialmedia_weibo;
