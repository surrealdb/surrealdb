/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = '''[{ details: '', error: "The `SEARCH` index has been renamed to `FULLTEXT` and some of it's parameters have been removed", kind: 'search index', location: { column: 21, kind: 'error', length: 24, line: 1, source: 'DEFINE INDEX f ON t1 FULLTEXT ANALYZER az VS', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 't1', 'index', 'f'], severity: 'unlikely_break' }]'''

*/

DEFINE INDEX f ON t1 SEARCH ANALYZER az VS DOC_IDS_ORDER 50;
migration::diagnose()
