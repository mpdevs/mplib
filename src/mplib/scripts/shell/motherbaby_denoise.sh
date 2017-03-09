USE transforms;
ADD FILE /home/udflib/udf_denoise_motherbaby_201703090121.py;
DROP TABLE IF EXISTS motherbaby_post_noise;
CREATE TABLE motherbaby_post_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703090121.py' AS (id, attr) FROM motherbaby_post;
