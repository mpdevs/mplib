USE transforms;
DROP TABLE IF EXISTS medicine_inquiry_user_noise;

CREATE TABLE medicine_inquiry_user_noise AS
SELECT
    user_id,
    COUNT(*) AS cnt
FROM l_medicine.post_inquiry
WHERE noise = '1'
GROUP BY
    user_id
HAVING COUNT(*) >= 5;


USE l_medicine;
DROP TABLE IF EXISTS user_tmp;
CREATE TABLE user_tmp LIKE user;

INSERT INTO user_tmp
PARTITION (platform_id)
SELECT
    u.user_id,
    u.brief_intro,
    u.user_tags,
    u.user_name,
    u.detail_url,
    u.user_gender,
    u.user_birthday,
    u.user_age,
    u.user_level,
    u.baby_count,
    u.baby_info,
    u.baby_gender,
    u.baby_birthday,
    u.baby_agenow,
    u.ask_count,
    u.reply_count,
    u.post_count,
    u.reply_post_count,
    u.quality_post_count,
    u.best_answer_count,
    u.fans_count,
    u.following_count,
    u.device,
    u.address,
    u.tel,
    u.province,
    u.city,
    u.created_at,
    u.updated_at,
    CASE WHEN n.user_id IS NULL THEN 0 ELSE 1 END AS noise,
    u.platform_id
FROM l_medicine.user AS u
LEFT JOIN transforms.medicine_inquiry_user_noise AS n
ON u.user_id = n.user_id;

DROP TABLE user;
ALTER TABLE user_tmp RENAME TO user;

