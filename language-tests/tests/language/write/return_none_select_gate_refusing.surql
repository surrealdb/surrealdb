/**
[env]
imports = ["./return_none_select_gate_refusing_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
A select permission that holds a construct the lowering decides when the statement compiles (a stored function, a
subquery, a full-text match) is evaluated under `RETURN NONE` as it is under any output: the statement answers what
it answers when it returns the record, and the stored rows are the same.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ code: 1, id: fn_gate_state:1, touched: true }, { code: -1, id: fn_gate_state:2, touched: true }, { code: 3, id: fn_gate_state:3, touched: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ code: 1, id: sub_gate_state:1, touched: true }, { code: 2, id: sub_gate_state:2, touched: true }, { code: 3, id: sub_gate_state:3, touched: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: match_gate_state:1, text: 'alpha beta', touched: true }, { id: match_gate_state:2, text: 'gamma', touched: true }, { id: match_gate_state:3, text: 'alpha alpha', touched: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ code: 1, id: fn_gate:1, touched: false }, { code: 3, id: fn_gate:3, touched: false }, { code: 4, id: fn_gate:4, touched: false }]"

[[test.results]]
value = "[{ code: 1, id: sub_gate:1, touched: false }, { code: 3, id: sub_gate:3, touched: false }, { code: 3, id: sub_gate:4, touched: false }]"

[[test.results]]
value = "[]"
*/

UPDATE fn_gate SET touched = true RETURN NONE;
SELECT * FROM fn_gate_state ORDER BY id;
UPDATE sub_gate SET touched = true RETURN NONE;
SELECT * FROM sub_gate_state ORDER BY id;
UPDATE match_gate SET touched = true RETURN NONE;
SELECT * FROM match_gate_state ORDER BY id;
DELETE fn_gate:2 RETURN NONE;
DELETE sub_gate:2 RETURN NONE;
CREATE fn_gate:4 SET code = 4 RETURN NONE;
CREATE sub_gate:4 SET code = 3 RETURN NONE;
UPDATE fn_gate SET touched = false RETURN AFTER;
UPDATE sub_gate SET touched = false RETURN AFTER;
UPDATE match_gate SET touched = false RETURN AFTER;
