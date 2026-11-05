/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A field guard and a computed field are judged against the row's own
	fields, never against a parameter, so no user parameter can stand in for
	a guard's input or read a denied field's raw value. A parameter named
	after a field, with or without an underscore prefix and an ordinal, is an
	ordinary parameter: it changes nothing about what the caller may read,
	and reading it back gives the value the caller set or NONE. Denied
	fields are projected under an alias, which reports NONE.
"""

# 0: the caller's view before any parameter is set.
[[test.results]]
value = "[{ cc: NONE, is_owner: false, s: NONE }]"

# 1: LET
[[test.results]]
value = "NONE"

# 2: LET
[[test.results]]
value = "NONE"

# 3: LET
[[test.results]]
value = "NONE"

# 4: LET
[[test.results]]
value = "NONE"

# 5: the same view with parameters named after the guard's input and the guarded fields.
[[test.results]]
value = "[{ cc: NONE, is_owner: false, s: NONE }]"

# 6: the guard is judged on the row's `is_owner`, which the parameter does not change.
[[test.results]]
value = "[]"

# 7: each parameter reads back as what the caller set.
[[test.results]]
value = "[{ a: true, b: 'leak', p: 42 }]"

# 8: a parameter the caller never set is NONE, not a field's raw value.
[[test.results]]
value = "[{ raw: NONE }]"

# 9: LET
[[test.results]]
value = "NONE"

# 10: an ordinary underscore-prefixed parameter.
[[test.results]]
value = "[{ x: 26 }]"
*/

-- Before any parameter is set.
SELECT is_owner, secret AS s, c AS cc FROM t;

-- Parameters named after the guard's input and after the guarded fields.
LET $_is_owner0 = true;
LET $is_owner = true;
LET $_secret1 = 'leak';
LET $_c0 = 42;
SELECT is_owner, secret AS s, c AS cc FROM t;
SELECT secret AS s FROM t WHERE is_owner = true;

-- The parameters themselves read as what the caller set, nothing else.
SELECT $_is_owner0 AS a, $_secret1 AS b, $_c0 AS p FROM t;
SELECT $_c1 AS raw FROM t;

-- An ordinary underscore-prefixed parameter keeps working.
LET $_x = 5;
SELECT $_x + n AS x FROM t;
