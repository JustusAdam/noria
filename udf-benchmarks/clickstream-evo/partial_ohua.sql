
CREATE TABLE clicks
( user_id int
, pagetype int
, ts int
);

VIEW clickstream_ana:
    SELECT user_id, op_s_sequences_0_0(pagetype,ts)
    FROM clicks
    WHERE user_id = ?
    GROUP BY user_id;
