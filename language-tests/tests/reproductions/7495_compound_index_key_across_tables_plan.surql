/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The plan its sibling's rows were produced by. The condition reaches through a
linked record, so the two tables' indexes must stay separate index options: the
scan drives `idx_edge_in_type` and joins `idx_node_type` for the remote column.
Merged into one compound key the scan looks up ['mm-Mission', 'HAS'] against an
index whose leading column holds a record id, and the DELETE silently removes
nothing.

DELETE renders the same plan under every strategy, so the file is not pinned.
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
value = "[{ detail: { plan: { index: 'idx_edge_in_type', joins: [{ index: 'idx_node_type', operator: '=', value: 'mm-Mission' }], operator: 'join' }, table: 'edge' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"
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
