/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
	A field's `PERMISSIONS FOR select` guard holds in every scope a statement
	can read the field from, because the guard is applied to the row before
	any user expression sees it: a block, an IF, `$parent` at the statement's
	own level and inside a nested SELECT — as a path and as the whole row — a
	WHERE used as an oracle, a user-defined function reading the field off
	the row, a closure, and a projection function naming the field at run
	time. A computed field the caller may read stays readable in the same
	scopes. The same holds over a record-id range and over an edge table.
	Denied fields are projected under an alias throughout: the value a scope
	reads is what these pin, and an alias is how every engine reports it.
"""

# 0: a block reading `secret` gets the guarded value.
[[test.results]]
value = "[{ s: NONE }]"

# 1: an IF reading `secret` gets the guarded value.
[[test.results]]
value = "[{ s: NONE }]"

# 2: `$parent.secret` at the statement's own level.
[[test.results]]
value = "[{ s: NONE }]"

# 3: `$parent.secret` from a nested SELECT.
[[test.results]]
value = "[{ s: NONE }]"

# 4: `$parent` as a whole row from a nested SELECT is the guarded row: no `secret`.
[[test.results]]
value = "[{ row: { id: t:1, is_owner: false, n: 21, owner: user:owner } }]"

# 5: the WHERE oracle through `$parent` selects nothing.
[[test.results]]
value = "[]"

# 6: the WHERE oracle through the field selects nothing.
[[test.results]]
value = "[]"

# 7: a user-defined function reading `secret` off the row.
[[test.results]]
value = "[{ s: NONE }]"

# 8: a closure reading `$parent.secret`.
[[test.results]]
value = "[{ s: false }]"

# 9: a closure reading `secret`.
[[test.results]]
value = "[{ s: false }]"

# 10: the computed field the guard reads is readable from a block.
[[test.results]]
value = "[{ owner_flag: false }]"

# 11: and beside the guarded field.
[[test.results]]
value = "[{ is_owner: false, s: NONE }]"

# 12: a record-id range source.
[[test.results]]
value = "[{ s: NONE }]"

# 13: an edge table source, from a block.
[[test.results]]
value = "[{ s: NONE }]"

# 14: an edge table source, projected.
[[test.results]]
value = "[{ s: NONE }]"

# 15: the WHERE oracle through a user-defined function selects nothing.
[[test.results]]
value = "[]"

# 16: a projection function naming the field at run time gets the guarded value.
[[test.results]]
value = "[{ s: NONE }]"

# 17: the WHERE oracle through a projection function selects nothing.
[[test.results]]
value = "[]"
*/

-- A block reading the field.
SELECT { RETURN secret } AS s FROM t;

-- An IF reading the field.
SELECT IF true { secret } AS s FROM t;

-- `$parent` at the statement's own level.
SELECT $parent.secret AS s FROM t;

-- `$parent` from a nested SELECT, as a path and as the whole row.
SELECT (SELECT VALUE $parent.secret FROM ONLY t:1) AS s FROM t;
SELECT (SELECT VALUE $parent FROM ONLY t:1) AS row FROM t;

-- The WHERE clause as an oracle.
SELECT id FROM t WHERE $parent.secret = 'hidden';
SELECT id FROM t WHERE secret = 'hidden';

-- A user-defined function reading the field off the row.
SELECT fn::peek() AS s FROM t;

-- A closure, through `$parent` and through the bare field name.
SELECT array::any([1], |$x| $parent.secret = 'hidden') AS s FROM t;
SELECT array::any([1], |$x| secret = 'hidden') AS s FROM t;

-- The computed field the guard reads is visible in the same scopes.
SELECT { RETURN is_owner } AS owner_flag FROM t;
SELECT is_owner, secret AS s FROM t;

-- A record-id range and an edge table.
SELECT { RETURN secret } AS s FROM t:1..=9;
SELECT { RETURN secret } AS s FROM owns;
SELECT secret AS s FROM owns;

-- The WHERE clause as an oracle through a user-defined function.
SELECT id FROM t WHERE fn::peek() = 'hidden';

-- A projection function naming the field at run time, projected and as an oracle.
SELECT type::field(IF n = 21 { 'secret' } ELSE { 'n' }) AS s FROM t;
SELECT id FROM t WHERE type::field(IF n = 21 { 'secret' } ELSE { 'n' }) = 'hidden';
