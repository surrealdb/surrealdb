/**
[test]
reason = "Test EXPLAIN with SELECT WHERE to show Filter operator"

[[test.results]]
value = """
"Filter [ctx: Db] [predicate: age > 18]
└────> ComputeFields [ctx: Db] [table: 'person']
       └────> Scan [ctx: Db] [table: 'person']
""""

*/

-- EXPLAIN with WHERE clause
EXPLAIN SELECT * FROM person WHERE age > 18;
