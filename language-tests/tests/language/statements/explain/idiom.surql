/**
[test]
reason = "Test EXPLAIN with idiom expressions"

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
    GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
      CurrentValueSource [ctx: Rt]
''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->knows->person->knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
    GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
      GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
        GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
          CurrentValueSource [ctx: Rt]
''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)<-knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
    GraphEdgeScan [ctx: Db] [direction: <-, tables: knows, output: TargetId]
      CurrentValueSource [ctx: Rt]
''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)<->knows<->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: <->, tables: person, output: TargetId]
    GraphEdgeScan [ctx: Db] [direction: <->, tables: knows, output: TargetId]
      CurrentValueSource [ctx: Rt]
''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->(SELECT * FROM knows WHERE age > 30)]
  expr.lookup: Project [ctx: Db]
    Filter [ctx: Db] [predicate: age > 30]
      GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: FullEdge]
        CurrentValueSource [ctx: Rt]
''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->(SELECT id, age FROM knows WHERE age > 30 GROUP BY age)->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
    Aggregate [ctx: Db] [by: age]
      Filter [ctx: Db] [predicate: age > 30]
        GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: FullEdge]
          CurrentValueSource [ctx: Rt]
''''

*/


EXPLAIN person:tobie->knows->person;

EXPLAIN person:tobie->knows->person->knows->person;

EXPLAIN person:tobie<-knows->person;

EXPLAIN person:tobie<->knows<->person;

EXPLAIN person:tobie->(SELECT * FROM knows WHERE age > 30);

EXPLAIN person:tobie->(SELECT id, age FROM knows WHERE age > 30 GROUP BY age)->person;