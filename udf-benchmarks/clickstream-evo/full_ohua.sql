CREATE TABLE clicks
( user_id int
, pagetype int
, ts int
);

VIEW clickstream_ana:
    SELECT click_ana(user_id,pagetype,ts)
    FROM clicks
    WHERE user_id = ?;
