USE transforms;
ADD FILE /home/udflib/udf_denoise_inquiry_201703090126.py;
DROP TABLE IF EXISTS medicine_post_inquiry_noise;
CREATE TABLE medicine_post_inquiry_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO medicine_post_inquiry_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_inquiry_201703090126.py' AS (id, attr) FROM medicine_post_inquiry;




