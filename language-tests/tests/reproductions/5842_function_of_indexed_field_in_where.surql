/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A predicate that wraps an indexed field in a function cannot use the index and still returns the matching rows"
issue = 5842

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:one, kind: 'banana' }]"

[[test.results]]
value = "[{ id: test:two, kind: 'apple' }]"

[[test.results]]
value = "[test:one]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: "string::contains(kind, 'nan')" }, children: [{ attributes: { direction: 'Forward', table: 'test' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""
*/

DEFINE TABLE test SCHEMALESS;
DEFINE INDEX kindIndex ON TABLE test COLUMNS kind;
CREATE test:one SET kind = "banana";
CREATE test:two SET kind = "apple";
SELECT VALUE id FROM test WHERE string::contains(kind, "nan");
SELECT VALUE id FROM test WHERE string::contains(kind, "nan") EXPLAIN;
