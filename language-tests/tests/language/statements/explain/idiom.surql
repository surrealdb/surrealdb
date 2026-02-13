/**
[test]
reason = "Test EXPLAIN with idiom expressions"

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->knows->person]\n  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]\n  └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]\n         └────> CurrentValueSource [ctx: Rt]\n''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->knows->person->knows->person]\n  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]\n  └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]\n         └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]\n                └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]\n                       └────> CurrentValueSource [ctx: Rt]\n''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)<-knows->person]\n  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]\n  └────> GraphEdgeScan [ctx: Db] [direction: <-, tables: knows, output: TargetId]\n         └────> CurrentValueSource [ctx: Rt]\n''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)<->knows<->person]\n  expr.lookup: GraphEdgeScan [ctx: Db] [direction: <->, tables: person, output: TargetId]\n  └────> GraphEdgeScan [ctx: Db] [direction: <->, tables: knows, output: TargetId]\n         └────> CurrentValueSource [ctx: Rt]\n''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->(SELECT * FROM knows WHERE age > 30)]\n  expr.lookup: Project [ctx: Db]\n  └────> Filter [ctx: Db] [predicate: age > 30]\n         └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: FullEdge]\n                └────> CurrentValueSource [ctx: Rt]\n''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->(SELECT id, age FROM knows WHERE age > 30 GROUP BY age)->person]\n  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]\n  └────> Aggregate [ctx: Db] [by: age]\n         └────> Filter [ctx: Db] [predicate: age > 30]\n                └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: FullEdge]\n                       └────> CurrentValueSource [ctx: Rt]\n''''

*/


EXPLAIN person:tobie->knows->person;

EXPLAIN person:tobie->knows->person->knows->person;


EXPLAIN person:tobie<-knows->person;

EXPLAIN person:tobie<->knows<->person;

EXPLAIN person:tobie->(SELECT * FROM knows WHERE age > 30);

EXPLAIN person:tobie->(SELECT id, age FROM knows WHERE age > 30 GROUP BY age)->person;