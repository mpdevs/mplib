-- 增量
USE transforms;
ADD FILE /home/udflib/udf_mother_baby_201711131137.py;
DROP TABLE IF EXISTS post_noise_v2_test;
CREATE TABLE post_noise_v2_test(id STRING, is_noise STRING) STORED AS ORC;
INSERT INTO post_noise_v2_test
SELECT TRANSFORM(content, id) USING 'python udf_denoise_motherbaby_201703161633.py' AS (is_noise, id)
FROM transforms.post_noise_v2_base;

-- 全量
USE l_motherbaby;
ADD FILE /home/udflib/mbpf1.py;
DROP TABLE IF EXISTS l_motherbaby.post_dnr_20180313;
CREATE TABLE l_motherbaby.post_dnr_20180313 LIKE l_motherbaby.post;
INSERT INTO l_motherbaby.post_dnr_20180313 PARTITION (platform_id)
SELECT TRANSFORM(
    id,
    channel,
    subject,
    post_id,
    title,
    tags,
    reply_count,
    view_count,
    collection_count,
    detail_url,
    content,
    is_best_answer,
    like_count,
    user_id,
    user_name,
    user_type,
    is_host,
    replied_user_id,
    replied_user_name,
    created_at,
    device,
    updated_at,
    baby_agethen,
    baby_days,
    floorid,
    noise,
    platform_id
)
USING 'python mbpf1.py' AS (
    id,
    channel,
    subject,
    post_id,
    title,
    tags,
    reply_count,
    view_count,
    collection_count,
    detail_url,
    content,
    is_best_answer,
    like_count,
    user_id,
    user_name,
    user_type,
    is_host,
    replied_user_id,
    replied_user_name,
    created_at,
    device,
    updated_at,
    baby_agethen,
    baby_days,
    floorid,
    noise,
    platform_id
)
FROM l_motherbaby.post;


