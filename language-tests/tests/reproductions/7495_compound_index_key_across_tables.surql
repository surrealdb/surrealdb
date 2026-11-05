/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A condition that reaches through a linked record resolves indexes on two
tables at once: the compound index on `edge` for the local column, and the
index on `node` for the remote column. The two must stay separate index
options - merged, the remote column's value is written into the compound key's
leading slot and the scan looks up ['mm-Mission', 'HAS'] against an index whose
leading column holds a record id, matching nothing. The DELETE then reports
success and removes nothing.
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
value = "[{ edge_type: 'HAS', id: edge:has, in: node:mission, out: node:craft }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE node SCHEMALESS;
DEFINE TABLE edge SCHEMALESS;
DEFINE FIELD in ON edge TYPE record<node>;
DEFINE FIELD edge_type ON edge TYPE string;
-- `idx_edge_in_type` and `idx_node_type` sit at the same offset in their own
-- table's index array, and index ids are allocated per table: the two indexes
-- are only told apart by carrying the table they belong to.
DEFINE INDEX idx_edge_in_type ON edge FIELDS in, edge_type;
DEFINE INDEX idx_node_type ON node FIELDS node_type;

CREATE node:mission SET node_type = 'mm-Mission';
CREATE node:craft;

RELATE node:mission -> edge:has -> node:craft CONTENT { edge_type: 'HAS' };

SELECT * FROM edge WHERE edge_type = 'HAS' AND in.node_type = 'mm-Mission';
DELETE edge WHERE edge_type = 'HAS' AND in.node_type = 'mm-Mission';
SELECT * FROM edge;
