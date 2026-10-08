/**
[env]
namespace = true
database = true
auth = { level = "owner" }
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A `VERSION` clause is evaluated once, by the `VersionScope` the planner
	wraps the statement in, against the unversioned outer context. A scan
	beneath it reads that stamp rather than evaluating the clause again,
	since the second evaluation would run at the stamp the first produced —
	where a `DEFINE PARAM` stored after it cannot be read, and where a
	volatile clause would answer with a different snapshot. Every other scan
	resolves it that way; the dynamic scan now does too, at each of the three
	places it needs the stamp.

	A `VERSION` datetime truncates to the HLC millisecond, so a read at a
	stamp does not see commits from the millisecond the stamp names. The
	definitions the read must resolve — the table and the function naming it —
	are therefore separated from the stamp by a sleep, as every other
	versioned test in this suite separates its revisions.
"""

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $hist]
    SelectProject [ctx: Db] [projections: *]
        Values [ctx: Rt] [expr: type::table(fn::name())]
''''

[[test.results]]
value = "[{ id: t:1, k: 1 }]"
*/

CREATE t:1 SET k = 1;
DEFINE FUNCTION fn::name() { RETURN 't' };

-- The stamp must name a later millisecond than the definitions above, which
-- a read at it would otherwise not see.
SLEEP 50ms;

-- Stored after the table exists, so the stamp names a database that holds
-- `t` — and strictly before the revision the parameter itself is stored at,
-- so a read of `$hist` at `$hist` finds no definition and answers `NONE`.
DEFINE PARAM $hist VALUE time::now() PERMISSIONS FULL;

-- A written-down table plans a `TableScan`, which reads the stamp.
SELECT * FROM t VERSION $hist;

-- A source the rules cannot fold stays a `DynamicScan`. It resolves to the
-- same table and owes the same rows.
EXPLAIN SELECT * FROM type::table(fn::name()) VERSION $hist;
SELECT * FROM type::table(fn::name()) VERSION $hist;
