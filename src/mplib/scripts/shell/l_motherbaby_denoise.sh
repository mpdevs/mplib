USE l_motherbaby;

INSERT INTO post_inquiry
SELECT
    s.id,
    s.channel,
    s.subject,
    s.post_id,
    s.title,
    s.tags,
    s.reply_count,
    s.view_count,
    s.collection_count,
    s.detail_url,
    s.content,
    s.is_best_answer,
    s.like_count,
    if(s.user_id = concat(s.platform_id, ":"), '', user_id) AS user_id,
    s.user_name,
    s.user_type,
    s.is_host,
    s.replied_user_id,
    s.replied_user_name,
    s.created_at,
    s.device,
    s.updated_at,
    s.baby_agethen,
    s.baby_days,
    s.floorid,
    CASE WHEN n.is_noise = "False" THEN '0' ELSE '1' END AS noise,
    s.platform_id
FROM transforms.motherbaby_post AS s
LEFT JOIN transforms.motherbaby_post_noise AS n
ON s.id = n.id;
