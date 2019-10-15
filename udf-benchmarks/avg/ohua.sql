CREATE TABLE Tab (aid int, ct int);

QUERY SumCount:
    SELECT aid, avg(aid, ct)
    FROM Tab
    WHERE aid = ?;
