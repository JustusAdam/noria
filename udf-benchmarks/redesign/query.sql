CREATE TABLE clicks
(
  cid int primary key
, user_id int
, pagetype int
, ts int
);

VIEW clickstream_ana:
    SELECT user_id, click_ana(user_id,pagetype,ts)
    FROM clicks
    WHERE user_id = ?;
