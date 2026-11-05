/**
[test]
reason = "Top-level `LET $rows = SELECT ...` runs the RHS through the planner. EXPLAIN ANALYZE of the same SELECT renders a plan tree"

[env]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 5}
    TableScan [ctx: Db] [table: seeded, direction: Forward] {rows: 5}

Total rows: 5''''

*/

{
    CREATE seeded:1 SET v = 1;
    CREATE seeded:2 SET v = 2;
    CREATE seeded:3 SET v = 3;
    CREATE seeded:4 SET v = 4;
    CREATE seeded:5 SET v = 5;
    RETURN NONE;
};

LET $rows = SELECT * FROM seeded;
RETURN array::len($rows);
RETURN (EXPLAIN ANALYZE SELECT * FROM seeded);
