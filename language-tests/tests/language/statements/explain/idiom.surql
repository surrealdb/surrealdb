/**
[test]
reason = "Test EXPLAIN with idiom expressions"

[[test.results]]
value = '''
"Expr [ctx: Db] [expr: (person:tobie)->knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [source: $__lookup_source__, direction: ->, tables: knows, output: TargetId]
  expr.lookup: GraphEdgeScan [ctx: Db] [source: $__lookup_source__, direction: ->, tables: person, output: TargetId]
"'''

*/


EXPLAIN person:tobie->knows->person;
