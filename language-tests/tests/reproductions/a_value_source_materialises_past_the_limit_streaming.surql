/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
	Half of a pair. Same queries as its sibling; the two files' expected
	results are the divergence, and the per-row engine's answer is the correct
	one.

	A value source -- a written array of record ids, a parameter holding one,
	or a call that returns one -- materialises every element before paging
	truncates it. Each materialisation derives the record's `COMPUTED`
	fields, so `LIMIT 1` over three ids advances a sequence three times where
	the statement owes one row. The per-row engine stops at the limit and
	draws once.

	The last query is the control that says this is about the source and not
	about paging: over a table source both engines draw once, so the streaming
	planner already bounds the work where the source is a scan.

	The field is projected deliberately. `SELECT id` would confound this with
	a separate divergence -- the per-row engine derives a `COMPUTED` field
	only on a demand it sees, so it would draw nothing at all and the counts
	would differ for a reason that has nothing to do with the limit.

	Recorded rather than fixed: bounding the work means pushing paging into a
	value source, which the planner does not do today, and doing it in the
	dynamic scan alone would leave the written-array path diverging. Read this
	file next to its pair rather than on its own.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "0"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ n: 5 }]"

[[test.results]]
value = "8"

[[test.results]]
value = "[{ n: 9 }]"

[[test.results]]
value = "10"










*/

DEFINE SEQUENCE s BATCH 1000 START 0;
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD n ON t COMPUTED sequence::nextval("s");
CREATE t:1, t:2, t:3 RETURN NONE;

-- The baseline draw: creating a record derives none of its computed fields.
RETURN sequence::nextval("s");

-- A written array of record ids.
SELECT n FROM [t:1, t:2, t:3] LIMIT 1;
RETURN sequence::nextval("s");

-- A call returning the same ids.
SELECT n FROM (|| [t:1, t:2, t:3])() LIMIT 1;
RETURN sequence::nextval("s");

-- The control: a table source bounds the work on both engines.
SELECT n FROM t LIMIT 1;
RETURN sequence::nextval("s");

