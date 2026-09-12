/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to compiled_closure_value: a RETURN whose expression
	compares, deduplicates or searches closure values plans as a Return above
	that expression, with closure literals carried as values rather than
	unfolded. A SELECT VALUE over a record id plans as a projection above the
	record's lookup, and a RETURN of a closure call plans as a Return above
	the call expression.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: $f == $f]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: array::distinct([$f, $f]).len()]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: [$f] CONTAINS $f]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: |$x: any| $x == |$x: any| $x]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: |$x: any| $x == |$y: any| $y]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 'test', id: t:1 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: d]
    RecordIdScan [ctx: Db] [record_id: t:1]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: $g()]
''''
*/

LET $f = |$x| $x;
EXPLAIN RETURN $f == $f;
EXPLAIN RETURN array::distinct([$f, $f]).len();
EXPLAIN RETURN [$f] CONTAINS $f;
EXPLAIN RETURN (|$x| $x) == (|$x| $x);
EXPLAIN RETURN (|$x| $x) == (|$y| $y);
LET $g = || session::db();
CREATE t:1 SET d = $g();
EXPLAIN SELECT VALUE d FROM t:1;
EXPLAIN RETURN $g();
