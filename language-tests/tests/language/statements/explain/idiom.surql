/**
[env]

[test]
reason = "Test EXPLAIN with idiom expressions. `->edge->vertex` pairs collapse to a single GraphEdgeScan in TargetVertex mode (the outer scan walks the edge table and emits the embedded target vertex), so the first two cases show `tables: knows` for the outer hop. Mixed-direction (`<-edge->`), `<->` (rejected from the fast path by `Dir::Both`), and inner-subquery cases stay on the two-scan plan."

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: (person:tobie)->knows->person]
''''

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: (person:tobie)->knows->person->knows->person]
''''

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: (person:tobie)<-knows->person]
''''

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: (person:tobie)<->knows<->person]
''''

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: (person:tobie)->(SELECT * FROM knows WHERE age > 30)]
''''

[[test.results]]
value = '''
'ExprPlan [ctx: Rt] [expr: (person:tobie)->(SELECT id, age FROM knows WHERE age > 30 GROUP BY age)->person]
''''

*/


EXPLAIN person:tobie->knows->person;

EXPLAIN person:tobie->knows->person->knows->person;

EXPLAIN person:tobie<-knows->person;

EXPLAIN person:tobie<->knows<->person;

EXPLAIN person:tobie->(SELECT * FROM knows WHERE age > 30);

EXPLAIN person:tobie->(SELECT id, age FROM knows WHERE age > 30 GROUP BY age)->person;
