/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "count() GROUP ALL under a filter on an unindexed field counts the rows the filter admits, and under a filter an index answers it counts from the index; a write to an unrelated record committed beside it is visible to the next read. Fair scheduling of concurrent scans is the VM's fuel meter, which no single-session result shows."
issue = 7358

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: resolves_to:5, status: 'current' }]"

[[test.results]]
value = "[{ total: 10 }]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: "source = 's3'", table: 'observation' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Aggregate' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ total: 10 }]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { condition: "source = 's3'", source: 'observation' }, context: 'Db', operator: 'IndexCountScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[]"

[[test.results]]
value = "['historical']"
*/

FOR $i IN 0..40 { CREATE type::record('observation', $i) SET source = 's' + <string>($i % 4); };
CREATE resolves_to:5 SET status = 'current';
SELECT count() AS total FROM observation WHERE source = 's3' GROUP ALL;
SELECT count() AS total FROM observation WHERE source = 's3' GROUP ALL EXPLAIN;
DEFINE INDEX ix_source ON observation FIELDS source;
SELECT count() AS total FROM observation WHERE source = 's3' GROUP ALL;
SELECT count() AS total FROM observation WHERE source = 's3' GROUP ALL EXPLAIN;
UPDATE resolves_to:5 SET status = 'historical' RETURN NONE;
SELECT VALUE status FROM resolves_to:5;
