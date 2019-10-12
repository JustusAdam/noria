
CREATE TABLE Tab (aid int, ct int);
VIEW TabSum:
    SELECT aid, op_acc_1_0(ct)
    FROM Tab
    WHERE aid = ?
    GROUP BY aid;