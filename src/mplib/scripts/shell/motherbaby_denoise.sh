USE transforms;
ADD FILE /home/udflib/udf_denoise_motherbaby_201703100451.py;

DROP TABLE IF EXISTS motherbaby_post_noise;

DROP TABLE IF EXISTS motherbaby_post_1001_noise;
CREATE TABLE motherbaby_post_1001_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1001_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1001';

DROP TABLE IF EXISTS motherbaby_post_1003_noise;
CREATE TABLE motherbaby_post_1003_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1003_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1003';

DROP TABLE IF EXISTS motherbaby_post_1004_noise;
CREATE TABLE motherbaby_post_1004_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1004_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1004';

DROP TABLE IF EXISTS motherbaby_post_1005_noise;
CREATE TABLE motherbaby_post_1005_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1005_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1005';

DROP TABLE IF EXISTS motherbaby_post_1006_noise;
CREATE TABLE motherbaby_post_1006_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1006_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1006';

DROP TABLE IF EXISTS motherbaby_post_1007_noise;
CREATE TABLE motherbaby_post_1007_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1007_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1007';

DROP TABLE IF EXISTS motherbaby_post_1008_noise;
CREATE TABLE motherbaby_post_1008_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1008_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1008';

DROP TABLE IF EXISTS motherbaby_post_1013_noise;
CREATE TABLE motherbaby_post_1013_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1013_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1013';

DROP TABLE IF EXISTS motherbaby_post_1015_noise;
CREATE TABLE motherbaby_post_1015_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1015_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1015';

DROP TABLE IF EXISTS motherbaby_post_1016_noise;
CREATE TABLE motherbaby_post_1016_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1016_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1016';

DROP TABLE IF EXISTS motherbaby_post_1017_noise;
CREATE TABLE motherbaby_post_1017_noise(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO motherbaby_post_1017_noise
SELECT TRANSFORM(id, content) USING 'python udf_denoise_motherbaby_201703100451.py' AS (id, is_noise)
FROM motherbaby_post
WHERE platform_id = '1017';