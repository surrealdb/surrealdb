/**
[env]
imports = ["language/indexes/full_text/matches_value_rows_import.surql"]
auth = { level = "owner" }

[test]
reason = """
	A row a subquery in `FROM` projects is judged on its own content: a
	`MATCHES` over a field the subquery projects as a literal reads that
	literal rather than the index entry of the record the row came from, and a
	field the subquery passes through is still answered by the index. A value
	row beside a table source is judged on its content whichever order the
	sources are written in. A subquery that passes the field through still
	has the index answer it.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:1, text: 'a quick probe' }]"

[[test.results]]
value = "[{ id: doc:1, text: 'the quick brown fox' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ text: 'the quick brown fox' }, { text: 'a quick probe' }]"

[[test.results]]
value = "[{ text: 'the quick brown fox' }]"

[[test.results]]
value = "[{ text: 'a quick probe' }, { text: 'the quick brown fox' }]"

[[test.results]]
value = "[{ text: 'the quick brown fox' }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: id, text]
            FullTextScan [ctx: Db] [table: doc, index: ft, query: quick]
''''

*/
SELECT * FROM (SELECT id, 'unrelated' AS text FROM doc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, 'a quick probe' AS text FROM doc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, text FROM doc) WHERE text @@ 'quick';
SELECT * FROM (SELECT id, text FROM doc) WHERE text @@ 'zebra';
SELECT text FROM doc, [{ id: doc:1, text: 'a quick probe' }] WHERE text @@ 'quick';
SELECT text FROM doc, [{ id: doc:1, text: 'completely unrelated content' }] WHERE text @@ 'quick';
SELECT text FROM [{ id: doc:1, text: 'a quick probe' }], doc WHERE text @@ 'quick';
SELECT text FROM [{ id: doc:1, text: 'completely unrelated content' }], doc WHERE text @@ 'quick';
EXPLAIN SELECT * FROM (SELECT id, text FROM doc) WHERE text @@ 'quick';
