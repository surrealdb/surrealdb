/**
[env]
imports = ["reproductions/layers_rederive_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
	A computed field is re-derived for the caller against the record as the
	caller may read it, whether or not the field carries a guard of its own:
	a self-dereference through the record's id reads the caller's view, so
	a denied field of the same record is `NONE` there, a dereference of a
	linked record is permission-checked, so a denied field of that record is
	`NONE` too, and a body naming the denied field reads `NONE`. A computed
	field's own guard is judged on the re-derived value as `$value`. A
	predicate on a re-derived field compares the re-derived value.
"""

[[test.results]]
value = "[{ link_hidden: NONE, link_hidden_open: NONE, self_g: NONE, self_g_open: NONE }]"

[[test.results]]
value = "[{ g_len: 0 }]"

[[test.results]]
value = "[{ f: friend:1, g_len: 0, id: item:1, link_hidden: NONE, link_hidden_open: NONE, self_g: NONE, self_g_open: NONE }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:1 }]"

*/

SELECT self_g, self_g_open, link_hidden, link_hidden_open FROM item;
SELECT g_len FROM item;
SELECT * FROM item;
SELECT id FROM item WHERE self_g = 'x';
SELECT id FROM item WHERE self_g = NONE;
