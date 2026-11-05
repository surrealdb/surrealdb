/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "EXPLAIN ANALYZE FORMAT JSON of a statement a block runs answers the JSON of its plan with the metrics its run recorded, as at the top level"

[[test.results]]
value = "[{ d: 3, id: o:1 }]"

[[test.results]]
value = "[{ d: 4, id: o:2 }]"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'o' }, context: 'Db', metrics: { output_rows: 2 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'SelectProject', total_rows: 2 }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: 'd > 1', table: 'o' }, context: 'Db', metrics: { output_rows: 2 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'SelectProject', total_rows: 2 }"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 2}
    TableScan [ctx: Db] [table: o, direction: Forward] {rows: 2}

Total rows: 2''''
*/
CREATE o:1 SET d = 3;
CREATE o:2 SET d = 4;
{ EXPLAIN ANALYZE FORMAT JSON SELECT * FROM o };
{ LET $x = 1; EXPLAIN ANALYZE FORMAT JSON SELECT * FROM o WHERE d > $x };
{ EXPLAIN ANALYZE SELECT * FROM o };
