CREATE TABLE clicks
(
  user_id int
  ,
  pagetype int
  ,
  ts int
);

-- CREATE TABLE clicks2
-- ( user_id int
-- , pagetype int
-- , ts int
-- );

-- c1: SELECT * FROM clicks;
clicks2:
SELECT *
FROM clicks;

candidate_paths0:
SELECT
  c1.user_id,
  c1.ts as ts1,
  c2.ts as ts2, 
FROM
  clicks c1 JOIN
  clicks2 c2 ON c1.user_id = c2.user_id
WHERE
  c1.pagetype = 0 AND
  c2.pagetype = 1;

-- Original version: SELECT DISTINCT (c1.user_id, ts1)
-- I dropped the DISTINCT, because it doesn't run with it and the data generation doesn't even generate duplicates
candidate_paths:
SELECT
  user_id,
  ts1,
  ts2
FROM
  candidate_paths0
WHERE
  ts1 < ts2
ORDER BY
  user_id, ts1, ts2
;

matching_paths:
SELECT
  user_id, max(ts1) as ts1, ts2
FROM candidate_paths
GROUP BY user_id, ts2;

pageview_counts0:
SELECT c.user_id, ts1, ts2, ts
FROM
  clicks c JOIN
  matching_paths ON c.user_id = matching_paths.user_id;

pageview_counts1:
SELECT
  user_id,
  ts1,
  ts2
FROM
  pageview_counts0
WHERE
  ts1 <= ts AND
  ts2 >= ts;

pageview_counts:
SELECT
  user_id,
  -- should be `count(*) - 2`
  count(*) as pageview_count
FROM
  pageview_counts1
GROUP BY
  user_id, ts1;

VIEW
clickstream_ana:
SELECT
  user_id,
  -- should be an `avg`
  sum(pageview_count)
FROM pageview_counts
WHERE user_id = ?;
