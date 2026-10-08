/**
[env]

[test]
reason = "EXPLAIN written inside a subquery source describes that subquery's own plan, and the outer statement selects from the description"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "['SelectProject']"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: *]
            TableScan [ctx: Db] [table: item, direction: Forward, predicate: score > 15, pre_decode_filter: yes]
''''

*/

{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	RETURN 'OK';
};

-- The inner EXPLAIN describes the subquery, and the outer SELECT reads its
-- description as rows.
SELECT VALUE operator FROM (SELECT * FROM item WHERE score > 15 EXPLAIN);

-- A leading EXPLAIN describes the whole statement, subquery source included,
-- and renders the tree as indented text rather than as a nested object.
EXPLAIN SELECT * FROM (SELECT * FROM item WHERE score > 15);
