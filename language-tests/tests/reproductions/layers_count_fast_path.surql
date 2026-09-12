/**
[env]
imports = ["reproductions/layers_count_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
	A computed field is derived only where something reads it: the statement,
	or a field guard the statement brings into the plan. A count reads no
	field, so a computed body that cannot produce a value of the field's
	declared type fails the statements that read the field and no other: the
	count and a read of a stored field are unaffected, and the failing read
	names the field. The plan companion pins the count path.
"""

# 0: the count.
[[test.results]]
value = "[{ count: 2 }]"

# 1: the computed field while its body derives an int.
[[test.results]]
value = "[{ n: 1 }, { n: 1 }]"

# 2: the write that makes the body derive a string.
[[test.results]]
value = "[{ id: other:1, v: 'x' }]"

# 3: the count is unaffected.
[[test.results]]
value = "[{ count: 2 }]"

# 4: a read of a stored field is unaffected.
[[test.results]]
value = "[{ a: 1 }, { a: 2 }]"

# 5: the read of the computed field fails, naming the field.
[[test.results]]
error = "Couldn't coerce value for field `n` of `t:1`: Expected `int` but found `'x'`"

# 6: a count whose predicate reads the field derives it and fails the same way.
[[test.results]]
error = "Couldn't coerce value for field `n` of `t:1`: Expected `int` but found `'x'`"
*/

-- The body derives a value of the declared type.
SELECT count() FROM t GROUP ALL;
SELECT n FROM t;

-- The record user makes the body derive a string for an int field.
UPDATE other:1 SET v = 'x';

-- Statements that do not read the field are unaffected.
SELECT count() FROM t GROUP ALL;
SELECT a FROM t;

-- The read of the field fails, naming the field.
SELECT n FROM t;
SELECT count() FROM t WHERE n > 0 GROUP ALL;
