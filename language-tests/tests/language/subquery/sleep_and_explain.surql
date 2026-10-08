/**
[test]
reason = "RETURN can wrap SLEEP (yields NONE) and EXPLAIN ANALYZE (yields the formatted plan) as scalar subqueries."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    RecordIdScan [ctx: Db] [record_id: subq:a] {rows: 1}

Total rows: 1''''

[env]

*/

{
    CREATE subq:a SET v = 1;
    RETURN NONE;
};

RETURN (SLEEP 1ms);
RETURN (EXPLAIN ANALYZE SELECT * FROM subq:a);
