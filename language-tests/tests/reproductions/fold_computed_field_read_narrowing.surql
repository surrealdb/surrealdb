/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Materialisation resolves the fields a query needs rather than all of them, so only a read of a field that carries work of its own is kept; an unrelated computed field elsewhere on the table must not cost the elimination"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Rt] [projections: id]\n    EmptyScan [ctx: Rt]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    TableScan [ctx: Db] [table: t, direction: Forward, predicate: bad INSIDE [], pre_decode_filter: no (computed fields)]\n''''

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Rt] [projections: id]\n    EmptyScan [ctx: Rt]\n''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: guarded_tb:1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Rt] [projections: id]\n    EmptyScan [ctx: Rt]\n''''

*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 5;
DEFINE FIELD bad ON t COMPUTED 1 + 1;

-- `a` computes nothing, so reading it stays a plain key lookup and the
-- predicate still collapses.
EXPLAIN SELECT id FROM t WHERE a IN [];

-- `bad` is what materialisation would evaluate, so that read is kept.
EXPLAIN SELECT id FROM t WHERE bad IN [];

-- A conditional permission is work only when enforcement is on for the
-- session; these tests run as owner, where the scan never evaluates one.
-- Both levels are checked, because they are separate gates.
DEFINE FIELD guarded ON t PERMISSIONS FOR select WHERE a > 0;
EXPLAIN SELECT id FROM t WHERE a IN [];

DEFINE TABLE guarded_tb SCHEMALESS PERMISSIONS FOR select WHERE a > 0;
CREATE guarded_tb:1 SET a = 5;
EXPLAIN SELECT id FROM guarded_tb WHERE a IN [];
