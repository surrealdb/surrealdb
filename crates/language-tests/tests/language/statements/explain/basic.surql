/**
[test]

[[test.results]]
value = """[
"ComputeFields [ctx: Db] [table: 'person']
└────> Scan [ctx: Db] [table: 'person']
"
]"""

*/

-- Basic EXPLAIN
EXPLAIN SELECT * FROM person;
