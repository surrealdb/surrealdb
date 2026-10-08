/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The plan its sibling's rows were produced by. The condition reaches through a
linked record, whose index belongs to a different table, so the DELETE must
not merge the two tables' columns into one compound key: merged, the scan
looks up ['mm-Mission', 'HAS'] against an index whose leading column holds a
record id, and the DELETE silently removes nothing. The DELETE is planned as
a `Write` over a `TableScan` that the write's own condition filters, so the
plan names no index and no key can be merged; the sibling asserts the rows the
DELETE removes.
"""
issue = 7495

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: node:mission, node_type: 'mm-Mission' }]"

[[test.results]]
value = "[{ id: node:craft }]"

[[test.results]]
value = "[{ edge_type: 'HAS', id: edge:has, in: node:mission, out: node:craft }]"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'edge' }, context: 'Db', metrics: { output_rows: 0 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'SelectProject' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'WriteOperator', total_rows: 0 }"
*/

DEFINE TABLE node SCHEMALESS;
DEFINE TABLE edge SCHEMALESS;
DEFINE FIELD in ON edge TYPE record<node>;
DEFINE FIELD edge_type ON edge TYPE string;
DEFINE INDEX idx_edge_in_type ON edge FIELDS in, edge_type;
DEFINE INDEX idx_node_type ON node FIELDS node_type;

CREATE node:mission SET node_type = 'mm-Mission';
CREATE node:craft;

RELATE node:mission -> edge:has -> node:craft CONTENT { edge_type: 'HAS' };

DELETE edge WHERE edge_type = 'HAS' AND in.node_type = 'mm-Mission' EXPLAIN FULL;
