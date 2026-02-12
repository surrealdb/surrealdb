/**
[test]
reason = "Test EXPLAIN with idiom expressions"

[[test.results]]
value = '''
"Expr [ctx: Db] [expr: (person:tobie)->knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
  └────> CurrentValueSource [ctx: Rt]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
  └────> CurrentValueSource [ctx: Rt]
"'''

*/


EXPLAIN person:tobie->knows->person;
