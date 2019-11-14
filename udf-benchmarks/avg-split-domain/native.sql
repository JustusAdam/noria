CREATE TABLE Tab (aid int, ct int);

SumQ:
    SELECT aid, sum(ct) as computed
    FROM Tab
    GROUP BY aid;

CountQ:
    SELECT aid, count(*) as computed
    FROM Tab
    GROUP BY aid;

QUERY SumCount:
    SELECT SumQ.aid, SumQ.computed / CountQ.computed
    FROM CountQ JOIN SumQ ON CountQ.aid = SumQ.aid
    WHERE SumQ.aid = ?;
