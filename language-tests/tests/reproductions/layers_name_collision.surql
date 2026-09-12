/**
[env]
imports = ["reproductions/layers_collision_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
	Every computed field and every field guard lands on the row under the
	field's own name, so two fields whose names differ only by a trailing
	digit — `a1` beside `a`, `_a1` beside `_a10` — are projected
	independently: the computed value is the computed field's and the denied
	stored field reads as NONE, whatever else the table declares. Denied
	fields are projected under an alias, which every engine reports as NONE.
"""

# 0: `a1` is the computed value, `a` is denied.
[[test.results]]
value = "[{ first: 1, second: NONE }]"

# 1: `_a1` is the computed value, `_a10` is denied.
[[test.results]]
value = "[{ first: 11, second: NONE }]"

# 2: the whole row: every computed field, neither denied field.
[[test.results]]
value = "[{ _a1: 11, a1: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8, i: 9, id: u:1, j: 10 }]"

# 3: the denied fields stay denied when the computed ones select the row.
[[test.results]]
value = "[{ x: NONE, y: NONE }]"
*/

SELECT a1 AS first, a AS second FROM u;
SELECT _a1 AS first, _a10 AS second FROM u;
SELECT * FROM u;
SELECT a AS x, _a10 AS y FROM u WHERE a1 = 1 AND _a1 = 11;
