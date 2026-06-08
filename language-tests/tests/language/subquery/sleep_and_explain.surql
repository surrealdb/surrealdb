/**
[test]
reason = "RETURN can wrap SLEEP (yields NONE) and EXPLAIN ANALYZE (yields the formatted plan) as scalar subqueries."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}\n    RecordIdScan [ctx: Db] [record_id: subq:a] {rows: 1}\n\nTotal rows: 1''''

*/

{
    CREATE subq:a SET v = 1;
    RETURN NONE;
};

RETURN (SLEEP 1ms);
RETURN (EXPLAIN ANALYZE SELECT * FROM subq:a);
