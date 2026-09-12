/**
[env]
imports = ["reproductions/layers_udf_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
	A user-defined function called from a table's `select` guard runs in the
	permission frame, where field guards do not apply: `fn::tier_of` reads a
	computed field denied to the caller, derived from a stored field denied
	to the caller, and still admits the member. The same function called by
	the caller runs in the caller's frame, where both guards apply. Denied
	fields are projected under an alias, which every engine reports as NONE.
"""

# 0: the table guard's function reads `tier` in the permission frame and admits the row.
[[test.results]]
value = "[{ id: doc:1, title: 'd' }]"

# 1: the same function in the caller's frame reads `tier` as NONE.
[[test.results]]
value = "'none'"

# 2: the caller's whole-row view: `active` and `tier` absent.
[[test.results]]
value = "[{ id: membership:1, who: user:reader }]"

# 3: the caller's projection: `active` and `tier` NONE.
[[test.results]]
value = "[{ a: NONE, t: NONE, who: user:reader }]"
*/

-- The guard's function reads `tier`, derived from `active`, and admits the row.
SELECT * FROM doc;

-- Called by the caller, the function reads the guarded row: `tier` is NONE.
RETURN fn::tier_of(user:reader);

-- The caller's own reads are guarded: `active` and `tier` are cut.
SELECT * FROM membership;
SELECT who, active AS a, tier AS t FROM membership;
