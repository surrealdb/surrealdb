/**
[test]
reason = "Test EXPLAIN FORMAT TEXT with simple SELECT statement"

[[test.results]]
value = """
"ComputeFields [ctx: Db] [table: 'person']
└────> Scan [ctx: Db] [table: 'person']
""""

*/

-- Explicit FORMAT TEXT
EXPLAIN FORMAT TEXT SELECT * FROM person;
