/**
[test]

[[test.results]]
value = """
"ComputeFields [ctx: Db] [table: 'person']
└────> Scan [ctx: Db] [table: 'person']
""""

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
'Let [name: $x]
=> Literal
'"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """
'Scalar => Param
'"""

[[test.results]]
value = """
'ComputeFields [ctx: Db] [table: $x]
└────> Scan [ctx: Db] [table: $x]
'"""

*/

-- Basic EXPLAIN
EXPLAIN SELECT * FROM person;

-- Explicit FORMAT TEXT
EXPLAIN FORMAT TEXT SELECT * FROM person;

-- EXPLAIN with WHERE clause
EXPLAIN SELECT * FROM person WHERE age > 18;

-- EXPLAIN with LET statement
EXPLAIN LET $x = 5;


LET $x = SELECT * FROM person;
EXPLAIN $x;
EXPLAIN SELECT * FROM $x;
