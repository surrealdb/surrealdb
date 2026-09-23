/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = """
	A user-defined function whose body is `{ RETURN <expr> }`, with
	`PERMISSIONS FULL` and an `AUTH LIMIT` that does not narrow the caller, is
	inlined at a call whose arguments are pure and whose coercion to the
	declared kinds is identity, so a predicate written as a call is the
	comparison it stands for and drives the field's index.

	The argument's kind is the checked one, not the declared one: a stored
	field's declaration is enforced on write, so a value written before it —
	or before `DEFINE FIELD OVERWRITE` retyped the field — holds what it held,
	and the coercion the call owes it is the one that reports the mismatch.
	A parameter declared `any` coerces every value by identity, so it inlines
	over a stored field and drives the index; a parameter declared `int` over
	a field only declared `int` keeps the call.

	A body with more than the return, a function the caller may not run, or an
	argument the declared kind would convert also keeps the call, evaluated
	per row.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: fn::is_target(n), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: = 5, direction: Forward, predicate: n = 5]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: fn::is_target_stepwise(n), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: fn::is_target_guarded(n), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: fn::is_target(s), pre_decode_filter: no (unsupported predicate)]
''''
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE FIELD s ON t TYPE string;
DEFINE INDEX idx_n ON t FIELDS n;
DEFINE FUNCTION fn::is_target($x: int) -> bool { RETURN $x = 5 };
DEFINE FUNCTION fn::is_target_stepwise($x: int) { LET $y = $x; RETURN $y = 5 };
DEFINE FUNCTION fn::is_target_guarded($x: int) { RETURN $x = 5 } PERMISSIONS NONE;
DEFINE FUNCTION fn::is_target_any($x: any) -> bool { RETURN $x = 5 };
-- `n` is declared `int`, not checked to be one, so the coercion stands.
EXPLAIN SELECT VALUE id FROM t WHERE fn::is_target(n);
-- `any` coerces every value by identity, so the call is its comparison and
-- the index answers it.
EXPLAIN SELECT VALUE id FROM t WHERE fn::is_target_any(n);
EXPLAIN SELECT VALUE id FROM t WHERE fn::is_target_stepwise(n);
EXPLAIN SELECT VALUE id FROM t WHERE fn::is_target_guarded(n);
EXPLAIN SELECT VALUE id FROM t WHERE fn::is_target(s);
