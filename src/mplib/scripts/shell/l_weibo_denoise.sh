USE l_socialmedia;

INSERT INTO weibo
SELECT
    s.id,
    s.platform_id,
    if(s.user_id = concat(s.platform_id, ":"), '', user_id) AS user_id,
    s.post_id,
    s.user_name,
    s.user_location,
    s.transmit_post_id,
    s.content,
    s.content_type,
    s.image_urls,
    s.device,
    s.verified_type,
    s.comment_count,
    s.like_count,
    s.transmit_count,
    s.detail_url,
    s.updated_at,
    s.created_at,
    CASE WHEN n.is_noise = "False" THEN '0' ELSE '1' END AS noise,
    s.daterange
FROM transforms.socialmedia_weibo AS s
LEFT JOIN transforms.socialmedia_weibo_noise AS n
ON s.id = n.id;

CREATE TABLE weibo_new LIKE weibo;
INSERT INTO weibo_new PARTITION (daterange)
SELECT
    s.id,
    s.platform_id,
    if(s.user_id = concat(s.platform_id, ":"), '', user_id) AS user_id,
    s.post_id,
    s.user_name,
    s.user_location,
    s.transmit_post_id,
    s.content,
    s.content_type,
    s.image_urls,
    s.device,
    s.verified_type,
    s.comment_count,
    s.like_count,
    s.transmit_count,
    s.detail_url,
    s.updated_at,
    s.created_at,
    CASE WHEN n.is_noise = "False" THEN '0' ELSE '1' END AS noise,
    s.daterange
FROM transforms.socialmedia_weibo AS s
LEFT JOIN transforms.socialmedia_weibo_noise AS n
ON s.id = n.id
WHERE s.daterange < '201601'
