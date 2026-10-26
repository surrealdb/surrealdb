/**
[env]
namespace = true
database = true

[test]
reason = "A COUNT over an index on an element column (`tags.*`) whose element field is declared a scalar kind, written with the plain containment `tags CONTAINS v` or `v IN tags`: a plain containment over an element column may be satisfied by a value that holds no element entry for v, so the count reads the records the index lists under v, `NONE` and `NULL` and judges the containment again over each. The counts match the records, a record repeating an element included."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ id: t:2, tags: ['a', 'a'] }]"

[[test.results]]
value = "[{ id: t:3, tags: ['c'] }]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { predicate: "tags CONTAINS 'a'" }, children: [{ attributes: { branches: '3', table: 't' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'ix', table: 't' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: '= NONE', direction: 'Forward', index: 'ix', table: 't' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: '= NULL', direction: 'Forward', index: 'ix', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'UnionIndexScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Aggregate' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { predicate: "'a' INSIDE tags" }, children: [{ attributes: { branches: '3', table: 't' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'ix', table: 't' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: '= NONE', direction: 'Forward', index: 'ix', table: 't' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: '= NULL', direction: 'Forward', index: 'ix', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'UnionIndexScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Aggregate' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD tags ON t TYPE array;
DEFINE FIELD tags.* ON t TYPE string;
DEFINE INDEX ix ON t FIELDS tags.*;
CREATE t:1 SET tags = ['a', 'b'];
CREATE t:2 SET tags = ['a', 'a'];
CREATE t:3 SET tags = ['c'];
SELECT count() FROM t WHERE tags CONTAINS 'a' GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE tags CONTAINS 'a' GROUP ALL;
SELECT count() FROM t WHERE 'a' IN tags GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE 'a' IN tags GROUP ALL;
SELECT count() FROM t WHERE tags CONTAINS 'c' GROUP ALL;
