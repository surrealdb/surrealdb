/**
[env]
imports = ["language/indexes/full_text/matches_value_rows_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	As matches_value_rows_inline, for a record user denied `select` on `doc`:
	a value row whose `id` names a `doc` record is not judged against `doc`'s
	index at all, in either source order, so even a row whose own content holds
	the word does not match, and a `doc` row it may not read contributes nothing.
	On `pdoc`, which it may read, a field a subquery projects as a literal is
	judged on that literal.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pdoc:1, text: 'a quick probe' }]"

*/
SELECT * FROM (SELECT id, 'unrelated' AS text FROM doc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, 'a quick probe' AS text FROM doc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, text FROM doc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, text FROM doc) WHERE text @@ 'zebra';
SELECT text FROM doc, [{ id: doc:1, text: 'a quick probe' }] WHERE text @@ 'quick';
SELECT text FROM doc, [{ id: doc:1, text: 'completely unrelated content' }] WHERE text @@ 'quick';
SELECT text FROM [{ id: doc:1, text: 'a quick probe' }], doc WHERE text @@ 'quick';
SELECT text FROM [{ id: doc:1, text: 'completely unrelated content' }], doc WHERE text @@ 'quick';
SELECT * FROM (SELECT id, 'unrelated' AS text FROM pdoc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, 'a quick probe' AS text FROM pdoc) WHERE text @@ 'quick';
