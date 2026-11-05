/**
[env]
imports = ["language/indexes/full_text/matches_value_rows_import.surql"]
auth = { level = "owner" }

[test]
reason = """
	An index answers a MATCHES only for a row that is a record it stores. A row
	built as a value — an object literal, a row beside a table scan — is judged on its own content even where its `id` names a stored record,
	and a `type::record` source is judged like the record id it reads.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[{ text: 'the quick brown fox' }]"

[[test.results]]
value = "[{ id: doc:1, m: true }]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[{ id: doc:1, s: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, m: true }]"

[[test.results]]
value = "[{ id: doc:1, m: true }]"

[[test.results]]
value = "[{ id: doc:1, m: true }]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[false]]"

[[test.results]]
value = "[cdoc:1, doc:1]"

[[test.results]]
value = "[true]"

*/
SELECT id FROM { id: doc:1, text: 'completely unrelated content' } WHERE text @@ 'quick';
SELECT id FROM { id: doc:1, text: 'a quick probe' } WHERE text @@ 'quick';
SELECT text FROM doc, [{ id: doc:1, text: 'completely unrelated content' }] WHERE text @@ 'quick';
SELECT id, text @@ 'quick' AS m FROM type::record('doc', 1);
SELECT id FROM type::record('doc', 1) WHERE text @@ 'quick';
SELECT id FROM type::table('doc') WHERE text @@ 'quick';
SELECT id, type::is_float(search::score(1)) AS s FROM doc WHERE text @1@ 'quick';
LET $t = 'doc';
SELECT id, text @@ 'quick' AS m FROM type::record($t, 1);
SELECT id, text @@ 'quick' AS m FROM type::record(doc:1);
SELECT id, text @@ 'quick' AS m FROM type::record('doc:1');
array::map([[{ id: doc:1, text: 'completely unrelated content' }]], |$r| (SELECT VALUE id FROM $r WHERE text @@ 'quick'));
SELECT VALUE (SELECT VALUE text @@ 'quick' FROM { id: doc:1, text: 'completely unrelated content' }) FROM doc WHERE text @@ 'quick';
SELECT VALUE id FROM cdoc, doc WHERE text @@ 'quick' ORDER BY id;
SELECT VALUE type::is_float(search::score(1)) FROM cdoc, doc WHERE text @1@ 'quick' AND id = doc:1;
