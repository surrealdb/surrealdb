/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Test EXPLAIN with idiom expressions. `->edge->vertex` pairs now collapse to a single GraphEdgeScan in TargetVertex mode (the outer scan walks the edge table and emits the embedded target vertex), so the first two cases show `tables: knows` rather than the previous `tables: person` for the outer hop. Mixed-direction (`<-edge->`), `<->` (rejected from the fast path by `Dir::Both`), and inner-subquery cases stay on the legacy two-scan plan."

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetVertex]
      CurrentValueSource [ctx: Rt]
''''

[[test.results]]
value = ''''Expr [ctx: Db] [expr: (person:tobie)->knows->person->knows->person]
  expr.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetVertex]
      GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetVertex]
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
  expr.lookup: SelectProject [ctx: Db] [projections: *]
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
