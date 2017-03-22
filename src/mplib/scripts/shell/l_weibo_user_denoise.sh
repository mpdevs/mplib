USE transforms;
DROP TABLE IF EXISTS socialmedia_weibo_user_noise;

CREATE TABLE socialmedia_weibo_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM l_socialmedia.weibo
WHERE noise = '1'
GROUP BY
    user_id
HAVING COUNT(*) >= 5;

USE l_socialmedia;
DROP TABLE IF EXISTS user_tmp;
CREATE TABLE user_tmp LIKE transforms.socialmedia_weibo_user;

INSERT INTO user_tmp
SELECT
    u.platform_id,
    u.user_id,
    u.user_name,
    u.user_real_name,
    u.user_location,
    u.user_gender,
    u.user_sex_orientation,
    u.user_relationship_status,
    u.user_birthday,
    u.user_level,
    u.blood_type,
    u.blog_url,
    u.weibo_personal_url,
    u.brief_intro,
    u.email,
    u.qq,
    u.msn,
    u.job_info,
    u.edu_info,
    u.tags,
    u.following_count,
    u.fans_count,
    u.blog_count,
    u.verified_intro,
    u.verified_type,
    u.detail_url,
    u.created_at,
    u.updated_at,
    CASE WHEN n.user_id IS NULL THEN 0 ELSE 1 END AS noise
FROM transforms.socialmedia_weibo_user AS u
LEFT JOIN transforms.socialmedia_weibo_user_noise AS n
ON u.user_id = n.user_id;

DROP TABLE user;
ALTER TABLE user_tmp RENAME TO user;
