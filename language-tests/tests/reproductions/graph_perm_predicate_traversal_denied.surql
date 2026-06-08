/**
[env]
imports = ["reproductions/graph_perm_predicate_traversal_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:other" }

[test]
reason = "Negative control for graph_perm_predicate_traversal: `user:other` is not a member of the workspace that owns the task's journey, so the permission predicate must evaluate to false and `task:t1` must be hidden. Confirms the skip_fetch_perms exemption does not make the predicate allow-all."

[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM task;
