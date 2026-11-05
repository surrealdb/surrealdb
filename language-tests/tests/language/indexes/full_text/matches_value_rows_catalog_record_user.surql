/**
[env]
imports = ["language/indexes/full_text/matches_value_rows_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A row judged on its own value reads the catalog of the table its `id` names
	only for a caller who may select the record it names. Each probe names `doc`,
	which has a full-text index on `text`, and then its twin `ndoc`, which has
	none: a caller denied `select` on both gets the same answer for each, so it
	learns nothing of their indexes, fields or analyzers. The record-link probes
	name `lnk` and `nlnk`, whose `link` fields point at `doc` and `ndoc`. A
	table whose `select` permission is `FULL` is judged against its index
	whether or not the record a row names exists: the caller reaches that index
	through the table itself.
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
value = "[false]"

[[test.results]]
value = "[false]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: fdoc:9 }]"
*/
SELECT id FROM { id: doc:1, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM { id: ndoc:1, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT id FROM [{ id: doc:2, text: 'a quick probe' }] WHERE text @@ 'quick';
SELECT id FROM [{ id: ndoc:2, text: 'a quick probe' }] WHERE text @@ 'quick';
SELECT VALUE text @@ 'quick' FROM { id: doc:1, text: 'a quick probe' };
SELECT VALUE text @@ 'quick' FROM { id: ndoc:1, text: 'a quick probe' };
SELECT id FROM { id: lnk:1, link: { text: 'a quick probe' } } WHERE link.text @@ 'quick';
SELECT id FROM { id: nlnk:1, link: { text: 'a quick probe' } } WHERE link.text @@ 'quick';
SELECT id FROM { id: fdoc:9, text: 'a quick probe' } WHERE text @@ 'quick';
