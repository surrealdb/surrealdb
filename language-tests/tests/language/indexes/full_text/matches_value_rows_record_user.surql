/**
[env]
imports = ["language/indexes/full_text/matches_value_rows_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A row a source builds as a value is judged on its own content, never on the
	full-text index entry of the record its `id` names: a caller denied `select`
	on that record learns nothing of what it stores, whichever word it probes for.
	Nor is it judged against that table's index at all, so even a row whose own
	content holds the word does not match.
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
value = "[[]]"

[[test.results]]
value = "[[[false]]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pdoc:1, s: true }]"

*/
SELECT id FROM { id: doc:1, text: 'completely unrelated content' } WHERE text @@ 'quick';
SELECT id FROM { id: doc:1, text: 'completely unrelated content' } WHERE text @@ 'zebra';
SELECT id FROM { id: doc:1, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM doc WHERE text @@ 'quick';
SELECT id FROM doc:1 WHERE text @@ 'quick';
array::map([[{ id: doc:1, text: 'completely unrelated content' }]], |$r| (SELECT VALUE id FROM $r WHERE text @@ 'quick'));
array::map(['user'], |$t| (SELECT VALUE (SELECT VALUE text @@ 'quick' FROM { id: doc:1, text: 'completely unrelated content' }) FROM type::table($t) WHERE id != NONE OR text @@ 'quick'));
SELECT id FROM doc, [{ id: doc:1, text: 'a quick probe' }] WHERE text @@ 'quick';
SELECT id FROM doc, [{ id: doc:1, text: 'completely unrelated content' }] WHERE text @@ 'quick';
SELECT id, type::is_float(search::score(1)) AS s FROM doc, pdoc WHERE text @1@ 'quick';
