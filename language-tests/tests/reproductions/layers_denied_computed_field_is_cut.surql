/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A computed field whose select guard is the literal `false` is denied to
	every caller outright. Reading it cuts it from the row, or reads it as `NONE`, rather than
	holding the denial to the field's declared kind: the field is declared
	`TYPE int` and a denied value is `NONE`, which is not an `int`, yet no
	read of the table raises. Denied fields are projected under an alias, as in
	layers_scopes.
"""

# 0: the whole row carries neither the denied computed field nor the guarded field.
[[test.results]]
value = "[{ id: t:1, is_owner: false, n: 21, owner: user:owner }]"

# 1: the denied field projected under an alias reads as NONE.
[[test.results]]
value = "[{ cc: NONE }]"

# 2: a permitted field beside it is unaffected.
[[test.results]]
value = "[{ cc: NONE, n: 21 }]"

# 3: the denied field read from a block is the guarded value.
[[test.results]]
value = "[{ v: NONE }]"
*/

SELECT * FROM t;
SELECT c AS cc FROM t;
SELECT n, c AS cc FROM t;
SELECT { RETURN c } AS v FROM t;
