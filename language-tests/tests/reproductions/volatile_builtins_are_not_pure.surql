/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = """
	A builtin that draws entropy, reaches past the transaction's snapshot or
	runs a body the call site cannot see is not pure, so decorrelation may
	not lift a subquery whose right side calls one: the lift builds the right
	relation once for every outer row to probe, where the scalar form builds
	a fresh one per outer row. The registry says which builtins those are,
	through `is_deterministic`.
"""

[[test.results]]
value = "[{ id: o:1, k: 1 }]"

[[test.results]]
value = "[{ id: r:1, k: 1 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: k]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: k = _join0.k]
        TableScan [ctx: Db] [table: o, direction: Forward]
        TableScan [ctx: Db] [table: r, direction: Forward, predicate: crypto::bcrypt::compare('h', 'p') != true, pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''
*/

CREATE o:1 SET k = 1;
CREATE r:1 SET k = 1;

-- Each `generate` draws a fresh salt, so the conjunct decides the one
-- relation every outer row probes rather than a fresh one per outer row.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE crypto::bcrypt::generate('p') != '');

-- `compare` reads the salt out of the hash it is handed and answers the
-- same every call, so the same shape still lifts.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE crypto::bcrypt::compare('h', 'p') != true);

-- The delay, not the `NONE`, is what the call is for.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE sleep(1ms) = NONE);

-- A remote endpoint is outside the transaction's snapshot.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE http::get('http://localhost/') != NONE);

-- So is a bucket.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE file::exists(f"b:/k") = true);
