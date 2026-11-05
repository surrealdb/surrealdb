/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A filter on one indexed field ordered by another under a LIMIT reads the filter's index and keeps the top rows, and a compound index over (filter, order) serves both, read backward for a descending order; each returns the rows the scan returns ordered and limited the same way."
issue = 6103

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[transaksi:28, transaksi:25, transaksi:22]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { limit: '3' }, children: [{ attributes: { keep: '3', sort_keys: 'created DESC' }, children: [{ attributes: { predicate: 'id > NONE' }, children: [{ attributes: { access: '= outlet:1', direction: 'Forward', index: 'ix_outlet', table: 'transaksi' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'Limit' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[transaksi:28, transaksi:25, transaksi:22]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[transaksi:28, transaksi:25, transaksi:22]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { limit: '3' }, children: [{ attributes: { predicate: 'id > NONE' }, children: [{ attributes: { access: '[outlet:1]', direction: 'Backward', index: 'ix_outlet_created', table: 'transaksi' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Limit' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE INDEX ix_created ON transaksi FIELDS created;
DEFINE INDEX ix_outlet ON transaksi FIELDS outlet;
FOR $i IN 0..30 { CREATE type::record('transaksi', $i) SET created = d'2025-01-01T00:00:00Z' + <duration>(<string>$i + 's'), outlet = type::record('outlet', $i % 3); };

SELECT VALUE id FROM transaksi WHERE id > NONE AND outlet = outlet:1 ORDER BY created DESC LIMIT 3;
SELECT VALUE id FROM transaksi WHERE id > NONE AND outlet = outlet:1 ORDER BY created DESC LIMIT 3 EXPLAIN;
SELECT VALUE id FROM transaksi WITH NOINDEX WHERE id > NONE AND outlet = outlet:1 ORDER BY created DESC LIMIT 3;
DEFINE INDEX ix_outlet_created ON transaksi FIELDS outlet, created;
SELECT VALUE id FROM transaksi WHERE id > NONE AND outlet = outlet:1 ORDER BY created DESC LIMIT 3;
SELECT VALUE id FROM transaksi WHERE id > NONE AND outlet = outlet:1 ORDER BY created DESC LIMIT 3 EXPLAIN;
