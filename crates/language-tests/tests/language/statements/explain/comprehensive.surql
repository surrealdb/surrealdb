/**
[test]

[[test.results]]
value = """
"ComputeFields [ctx: Db] [table: 'person']
└────> Scan [ctx: Db] [table: 'person']
""""

[[test.results]]
value = """
"Filter [ctx: Db] [predicate: age > 18]
└────> ComputeFields [ctx: Db] [table: 'person']
       └────> Scan [ctx: Db] [table: 'person']
""""

[[test.results]]
value = """
"Union [ctx: Db]
├────> ComputeFields [ctx: Db] [table: 'person']
│      └────> Scan [ctx: Db] [table: 'person']
└────> ComputeFields [ctx: Db] [table: 'company']
       └────> Scan [ctx: Db] [table: 'company']
""""

[[test.results]]
value = """
"Let [name: $data]
└────> ComputeFields [ctx: Db] [table: 'person']
       └────> Scan [ctx: Db] [table: 'person']
""""

[[test.results]]
value = """
'Scalar => BinaryOp
'"""

*/

-- Simple SELECT
EXPLAIN SELECT * FROM person;

-- SELECT with WHERE
EXPLAIN SELECT * FROM person WHERE age > 18;

-- SELECT with UNION (multiple sources)
EXPLAIN SELECT * FROM person, company;

-- LET with query
EXPLAIN LET $data = SELECT * FROM person;

-- Scalar expression
EXPLAIN 1 + 2;

