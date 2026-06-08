/**
[env]
backend = ["mem"]
versioned = true
timeout = 5000
imports = ["reproductions/graph_version_targetvertex_perm_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:alice" }

[test]
reason = "A `VERSION` traversal must enforce the edge table's historical SELECT permissions, not the current ones. Before the fix, `try_fast_path_pair` checked the current catalog (now `select FULL`) and admitted the two-hop `->friend->person` query to the TargetVertex fast path, which skips the edge record entirely and leaks `person:dave` to `person:alice` even though the historical `friend` SELECT was gated by `in.id = $auth.id`. Also exercises the scan operators' VERSION-stamp resolution: `VERSION $hist` resolves through a DB-level `DEFINE PARAM`, which used to fail in the streaming scans because they re-evaluated `version_expr` under the already-versioned context."

# Historical edge-only traversal: must enforce the historical
# `friend WHERE in.id = $auth.id` and exclude bob's edge from alice's
# view.
[[test.results]]
value = "[]"

# Historical two-hop traversal collapsed by the planner. The fast path
# used to leak `[person:dave]` here; with the fix the VERSION clause
# forces the slow path and the historical edge SELECT WHERE applies,
# returning `[]`.
[[test.results]]
value = "[]"

*/

SELECT VALUE ->friend FROM ONLY person:bob VERSION $hist;
SELECT VALUE ->friend->person FROM ONLY person:bob VERSION $hist;
