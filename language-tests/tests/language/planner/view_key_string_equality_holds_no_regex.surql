/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An aggregated view pushes a string equality on a group column into a key
	range although `=` also holds between a string and a regex that matches
	it, because no view row can be keyed by a regex: a regex cannot be stored
	in a record or in a record id, so a source write whose group value is a
	regex fails and writes no row. The string-keyed row is found by the key
	range, and the answer equals the one WITH NOINDEX gives.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Tried to serialize a value which cannot be serialized."

[[test.results]]
error = "field `id` of key `RecordKey`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, s: 'foo' }]"

[[test.results]]
value = "[{ id: src:2, s: 'foo' }]"

[[test.results]]
value = "[{ id: src:3, s: 'bar' }]"

[[test.results]]
value = "[{ id: w:['foo'], n: 2 }]"

[[test.results]]
value = "[{ id: w:['foo'], n: 2 }]"

[[test.results]]
value = "[{ id: w:['foo'], n: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, n]
    KeyRangeScan [ctx: Db] [table: w, key: id[0] = 'foo', direction: Forward, residual: yes, predicate: s = 'foo', pre_decode_filter: yes]
"'''

*/

DEFINE TABLE rsrc SCHEMALESS;
DEFINE TABLE v AS SELECT count() AS n, <regex> s AS x FROM rsrc GROUP BY x;
CREATE rsrc:1 SET s = 'fo+';
CREATE t:[/foo/];
SELECT * FROM rsrc;
SELECT * FROM v;
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE w AS SELECT count() AS n, s FROM src GROUP BY s;
CREATE src:1 SET s = 'foo';
CREATE src:2 SET s = 'foo';
CREATE src:3 SET s = 'bar';
SELECT id, n FROM w WHERE s = 'foo';
SELECT id, n FROM w WITH NOINDEX WHERE s = 'foo';
SELECT id, n FROM w WHERE s IN ['foo', 'baz'];
EXPLAIN SELECT id, n FROM w WHERE s = 'foo';
