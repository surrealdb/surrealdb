/**
[env]
imports = ["./return_none_select_gate_raises_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
A select permission that raises is evaluated even under `RETURN NONE`, which
shows no record: the statement fails with the error of the record the predicate
raised on, and the writes of the statement are undone. The predicate may raise
in the clause itself, in a computed field it reads, or only when the value the
field derives is coerced to the kind the field declares.
"""

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ code: 1, id: cast_gate_state:1, touched: NONE }, { code: 'x', id: cast_gate_state:2, touched: NONE }, { code: 3, id: cast_gate_state:3, touched: NONE }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `total` of `typed_gate:1`: Expected `int` but found `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ code: 1, id: cast_gate_state:1, touched: NONE }, { code: 'x', id: cast_gate_state:2, touched: NONE }, { code: 3, id: cast_gate_state:3, touched: NONE }]"

[[test.results]]
error = "Could not cast into `int` using input `'y'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ code: 1, id: cast_gate_state:1, touched: NONE }, { code: 'x', id: cast_gate_state:2, touched: NONE }, { code: 3, id: cast_gate_state:3, touched: NONE }, { code: 5, id: cast_gate_state:5, touched: NONE }]"
*/

UPDATE cast_gate SET touched = true RETURN NONE;
SELECT * FROM cast_gate_state ORDER BY id;
UPDATE derived_gate SET touched = true RETURN NONE;
UPDATE derived_gate:1 SET touched = true RETURN NONE;
UPDATE derived_gate:3 SET touched = true RETURN NONE;
UPDATE derived_gate:2 SET touched = true RETURN NONE;
UPDATE typed_gate SET code = 'x' RETURN NONE;
UPDATE typed_gate:1 SET touched = true RETURN NONE;
DELETE cast_gate RETURN NONE;
DELETE cast_gate:2 RETURN NONE;
SELECT * FROM cast_gate_state ORDER BY id;
CREATE cast_gate:4 SET code = 'y' RETURN NONE;
CREATE cast_gate:5 SET code = 5 RETURN NONE;
SELECT * FROM cast_gate_state ORDER BY id;
