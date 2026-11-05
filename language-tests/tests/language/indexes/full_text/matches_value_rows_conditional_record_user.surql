/**
[env]
imports = ["language/indexes/full_text/matches_value_rows_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Where a table's `select` permission is conditional, a row judged on its own
	value is judged against the table's full-text index only when the record its
	`id` names exists and the permission admits it as stored. The caller's own
	record is matched on the row's content, whether read by id or written as a
	value. A row naming another user's record, or no record at all, answers as
	it does over `nodoc`, the twin of `odoc` with no index, even when the row's
	own content would satisfy the permission.
"""

[[test.results]]
value = "[{ id: odoc:mine }]"

[[test.results]]
value = "[{ id: odoc:mine }]"

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
*/
SELECT id FROM odoc:mine WHERE text @@ 'quick';
SELECT id FROM { id: odoc:mine, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM { id: odoc:mine, text: 'unrelated' } WHERE text @@ 'quick';
SELECT id FROM { id: odoc:theirs, owner: user:reader, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM { id: nodoc:theirs, owner: user:reader, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM { id: odoc:none, owner: user:reader, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM { id: nodoc:none, owner: user:reader, text: 'a quick probe' } WHERE text @@ 'quick';
