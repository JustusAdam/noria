CREATE TABLE Tab (aid int, ct int);

QUERY SumCount:
    SELECT aid, sum_count(aid, ct)
    FROM Tab
    WHERE aid = ?;