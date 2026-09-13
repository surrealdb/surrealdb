/**
[env]
imports = ["reproductions/closure_call_guard_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
	The plan behind closure_callback_guard. A builtin that runs a closure it
	is handed runs a body the call site cannot see when the closure is not
	written in the call, so the predicate demands the whole row: the guard
	layer is emitted and the predicate is judged above it, as `$pred()` is.
	A builtin that runs no closure reads its arguments by name and is still
	offered to the scan, as is a closure-running builtin whose closure
	position holds a value that cannot be a closure: only that position is
	judged, so the array it searches may be a field.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: array::any([1], $pred)]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: array::any([1], $o.check)]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: [1].any($pred)]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: array::any([1], |$x: any| (secret = 'yes'))]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: string::len(public) > 0, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: public]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: array::includes(tags, 'x'), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: array::any(tags, $pred)]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

*/

LET $pred = |$x| secret = 'yes';
LET $o = { check: |$x| secret = 'yes' };

-- The closure as a parameter, as a field of an object, and as a method
-- argument: the guard layer is emitted under each.
EXPLAIN SELECT VALUE public FROM t WHERE array::any([1], $pred);
EXPLAIN SELECT VALUE public FROM t WHERE array::any([1], $o.check);
EXPLAIN SELECT VALUE public FROM t WHERE [1].any($pred);

-- Written in the call, the body reads `secret` by name and the layer is
-- emitted for that read.
EXPLAIN SELECT VALUE public FROM t WHERE array::any([1], |$x| secret = 'yes');

-- A builtin that runs no closure still reaches the scan.
EXPLAIN SELECT VALUE public FROM t WHERE string::len(public) > 0;

-- A closure-running builtin whose closure position holds a literal reads
-- only the array it searches, and reaches the scan; a closure in that
-- position over the same array is guarded.
EXPLAIN SELECT VALUE public FROM t WHERE array::includes(tags, 'x');
EXPLAIN SELECT VALUE public FROM t WHERE array::any(tags, $pred);
