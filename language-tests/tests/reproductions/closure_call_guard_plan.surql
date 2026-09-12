/**
[env]
imports = ["reproductions/closure_call_guard_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = "an invocation demands the whole row, so the guard layer is emitted and the predicate stays above it; a registered method still reads by name and still reaches the scan"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: $pred()]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: public]
    Filter [ctx: Db] [predicate: secret = 'yes']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: public]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: public.len() > 0, pre_decode_filter: no (unsupported predicate)]
''''
*/

LET $pred = || secret = 'yes';

-- The guard layer is emitted and the predicate is judged above it, exactly
-- as the predicate that names the field is.
EXPLAIN SELECT VALUE public FROM t WHERE $pred();

EXPLAIN SELECT VALUE public FROM t WHERE secret = 'yes';

-- A registered method is not an unseen body: it dispatches on the receiver
-- and reads by name, so it is still offered to the scan.
EXPLAIN SELECT VALUE public FROM t WHERE public.len() > 0;
