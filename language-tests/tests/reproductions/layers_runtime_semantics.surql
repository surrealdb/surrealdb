/**
[env]
imports = ["reproductions/layers_semantics_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	What a record user reads from a table with guarded and computed fields.
	A computed field's guard naming a denied stored field is judged against
	the stored value, so `WHERE g != 'x'` denies when `g` is `'x'` even
	though the caller never sees `g`. A computed field reading a denied
	stored field is derived from the row as the caller may see it, so it
	reads NONE; one dereferencing its own record reads that same view. A
	denied field is absent from `SELECT *`, not present as NONE. A guard on a
	wildcard path cuts the key from every element. A `FROM` naming two tables
	applies each table's guards to that table's rows. Denied fields are
	projected under an alias, which reports NONE.
"""

# 0: `g` and `c_raw` absent; `c_reads` re-derived to true; `c_self` reads the caller's view; `c_pass` admitted.
[[test.results]]
value = "[{ c_pass: 'P', c_reads: true, c_self: NONE, id: item:1, shown: 1 }]"

# 1: the same under aliases: the denied fields NONE.
[[test.results]]
value = "[{ admitted: 'P', raw_judged: NONE, rederived: true, self_view: NONE, stored: NONE }]"

# 2: the re-derived value selects the row.
[[test.results]]
value = "[{ c_pass: 'P', c_reads: true, c_self: NONE, id: item:1, shown: 1 }]"

# 3: the wildcard guard cuts `secret` from every element of `arr`.
[[test.results]]
value = "[{ arr: [{ shown: 1 }, { shown: 2 }], id: bag:1 }]"

# 4: projected.
[[test.results]]
value = "[{ arr: [{ shown: 1 }, { shown: 2 }] }]"

# 5: as a value.
[[test.results]]
value = "[[{ shown: 1 }, { shown: 2 }]]"

# 6: each table's denied field absent from its rows.
[[test.results]]
value = "[{ id: a:1, pub: 'a' }, { id: b:1, pub: 'b' }]"

# 7: each table's denied field NONE under an alias.
[[test.results]]
value = "[{ ha: NONE, hb: NONE, pub: 'a' }, { ha: NONE, hb: NONE, pub: 'b' }]"
*/

-- Denied fields are absent; computed fields carry the caller's view.
SELECT * FROM item;
SELECT g AS stored, c_raw AS raw_judged, c_reads AS rederived, c_self AS self_view, c_pass AS admitted FROM item;
SELECT * FROM item WHERE c_reads = true;

-- A wildcard guard cuts the key from every element.
SELECT * FROM bag;
SELECT arr FROM bag;
SELECT VALUE arr FROM bag;

-- Each table's guards apply to that table's rows.
SELECT * FROM a, b;
SELECT pub, hid_a AS ha, hid_b AS hb FROM a, b;
