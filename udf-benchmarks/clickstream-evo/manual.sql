
CREATE TABLE clicks
( user_id int
, pagetype int
, ts int
);

VIEW clickstream_ana:
SELECT user_id, click_ana_manual(pagetype,ts)
FROM clicks
WHERE user_id = ?
GROUP BY user_id;
