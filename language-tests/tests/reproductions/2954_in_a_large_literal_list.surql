/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "IN against a list bound to a parameter judges each row against the list. A computed string key compares against the list per row, and against a subquery over the list through the membership index the subquery keeps; the record ids themselves are read by key, one read per listed record, however many the list holds. Each returns the rows a filter over the gathered rows returns."
issue = 2954

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[e:3, e:5, e:17]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: "meta::tb(id) + ':' + <string> meta::id(id) INSIDE ['e:3', 'e:17', 'e:5', 'e:99']" }, children: [{ attributes: { direction: 'Forward', table: 'e' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = "[e:3, e:5, e:17]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { record_id: '[e:3, e:5, e:17, e:99]' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[e:3, e:5, e:17]"

[[test.results]]
value = "[e:3, e:5, e:17]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 10 }]"
*/

FOR $i IN 0..20 { CREATE type::record('e', $i) SET n = $i; };
LET $keys = ['e:3', 'e:17', 'e:5', 'e:99'];
LET $ids = [e:3, e:17, e:5, e:99];

SELECT VALUE id FROM e WHERE meta::tb(id) + ':' + <string>meta::id(id) IN $keys;
SELECT VALUE id FROM e WHERE meta::tb(id) + ':' + <string>meta::id(id) IN $keys EXPLAIN;
SELECT VALUE id FROM e WHERE id IN $ids;
SELECT VALUE id FROM e WHERE id IN $ids EXPLAIN;
RETURN (SELECT VALUE id FROM e)[WHERE $this IN $ids];
SELECT VALUE id FROM e WHERE meta::tb(id) + ':' + <string>meta::id(id) IN (SELECT VALUE $this FROM $keys);
LET $many = (<array> 0..40000).map(|$i| type::record('e', $i * 2));
SELECT count() FROM e WHERE id IN $many GROUP ALL;
