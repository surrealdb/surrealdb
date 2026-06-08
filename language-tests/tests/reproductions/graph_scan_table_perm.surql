/**
[env]
imports = ["reproductions/graph_scan_table_perm_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "node:a" }

[test]
reason = "Streaming graph (`->edge->...`) and reference traversal must enforce the edge/target table's SELECT permission. A record user must not be able to traverse into records on a `PERMISSIONS FOR select NONE` table — neither the IDs (default mode) nor the contents (`FullEdge` mode) should be observable."

[[test.results]]
value = "[{ edges: [] }]"

[[test.results]]
value = "[{ targets: [] }]"

[[test.results]]
value = "[]"
*/

-- Edge table denies SELECT, so the traversal must yield no edges. The
-- source row is still returned because `node` allows SELECT.
SELECT ->edge AS edges FROM node:a;
SELECT ->edge->node AS targets FROM node:a;
SELECT * FROM edge;
