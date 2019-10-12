CREATE TABLE clicks 
  ( user_id int
  , pagetype int
  , ts int(64)
  )

candidate_paths:
SELECT DISTINCT ON (c1.user_id, ts1)
  c1.user_id,
  c1.ts as ts1,
  c2.ts as ts2
FROM
  clicks c1,
  clicks c2
WHERE
  c1.user_id = c2.user_id AND
  c1.ts < c2.ts AND
  c1.pagetype = 0 AND
  c2.pagetype = 1
ORDER BY
  c1.user_id, c1.ts, c2.ts;

matching_paths:
SELECT
  user_id, max(ts1) as ts1, ts2
FROM candidate_paths
GROUP BY user_id, ts2;
    
pageview_counts:
SELECT
  c.user_id, matching_paths.ts1,
    count(*) - 2 as pageview_count
FROM
  clicks c, matching_paths
WHERE
  c.user_id = matching_paths.user_id AND
  c.ts >= matching_paths.ts1 AND
  c.ts <= matching_paths.ts2
GROUP BY
  c.user_id, matching_paths.ts1

VIEW clickstream_ana:
SELECT
    avg(pageview_count)
FROM pageview_counts;
