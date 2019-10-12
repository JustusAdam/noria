CREATE TABLE Tab (aid int, ct int);
VIEW TabSum:
    SELECT aid, count(ct)
    FROM Tab
    WHERE aid = ?
    GROUP BY aid;