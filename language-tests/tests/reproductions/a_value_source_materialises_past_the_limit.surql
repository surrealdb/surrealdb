/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A value source -- a written array of record ids, a parameter holding one, or a
call that returns one -- materialises every element before paging truncates
it. Each materialisation derives the record's `COMPUTED` fields, so `LIMIT 1`
over three ids advances a sequence three times where the statement owes one
row. The intended contract is that paging stops at the limit and draws once;
this file pins the current count.

The last query is the control that says this is about the source and not about
paging: over a table source the scan bounds the work and draws once.

The field is projected deliberately. `SELECT id` would demand no `COMPUTED`
field and draw nothing, so the counts would differ for a reason that has
nothing to do with the limit.

Bounding the work means pushing paging into a value source, which the planner
does not do; doing it in the dynamic scan alone would leave the written-array
path unbounded.
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

-- The control: a table source bounds the work.
SELECT n FROM t LIMIT 1;
RETURN sequence::nextval("s");

