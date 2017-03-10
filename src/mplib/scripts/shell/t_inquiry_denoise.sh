USE transforms;
ADD FILE /home/udflib/udf_denoise_inquiry_201703090126.py;
DROP TABLE IF EXISTS medicine_post_inquiry_noise;
CREATE TABLE medicine_post_inquiry_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO medicine_post_inquiry_noise
SELECT TRANSFORM(content, id) USING 'python udf_denoise_inquiry_201703090126.py' AS (id, is_noise) FROM medicine_post_inquiry;




