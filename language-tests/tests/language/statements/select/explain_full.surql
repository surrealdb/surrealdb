/**
[test]
reason = "Test SELECT ... EXPLAIN and EXPLAIN FULL to exercise the Explain enum"

[[test.results]]
value = "[{ id: item:1, name: 'test', val: 42 }]"

[[test.results]]
value = "[{ id: item:2, name: 'other', val: 99 }]"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', metrics: { output_rows: 2 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'SelectProject', total_rows: 2 }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', predicate: 'val > 50', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

*/

CREATE item:1 SET name = 'test', val = 42;
CREATE item:2 SET name = 'other', val = 99;

SELECT * FROM item EXPLAIN;
SELECT * FROM item EXPLAIN FULL;
SELECT * FROM item WHERE val > 50 EXPLAIN;
