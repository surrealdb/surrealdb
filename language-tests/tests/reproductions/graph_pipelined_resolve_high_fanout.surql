/**
[env]
imports = ["reproductions/graph_pipelined_resolve_high_fanout_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:viewer" }

[test]
reason = "High-fanout traversal as a record user: the edge batches cross the default `scan_batch_size` (1000), so the graph scan's pipelined record resolve engages — each flushed batch resolves (with the per-row edge SELECT permission applied) while the scan continues. Asserts nothing is dropped, duplicated, or reordered across the flush boundary."

# 0
[[test.results]]
value = "1200"

# 1
[[test.results]]
value = "[knows:999, knows:1000, knows:1001, knows:1002]"

# 2
[[test.results]]
value = "1200"
*/

-- 0: Edge-only (`->edge`) traversal. 1500 edges are scanned in batches
--    of 1000; the per-row permission keeps exactly the first 1200.
array::len((SELECT VALUE ->knows FROM ONLY person:source));

-- 1: The slice spanning the flush boundary (raw ids 1000/1001): edges
--    must stay complete and in adjacency order across the pipelined
--    resolve hand-off.
(SELECT VALUE ->knows FROM ONLY person:source)[998..1002];

-- 2: Full `->edge->vertex` traversal: only targets of visible edges
--    are reachable.
array::len((SELECT VALUE ->knows->person FROM ONLY person:source));
