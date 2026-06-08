/**
[env]
imports = ["reproductions/graph_perm_predicate_traversal_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:martin" }

[test]
reason = "A graph traversal *inside* a table's SELECT permission predicate must be evaluated unrestricted (the predicate is definer-authored and runs with skip_fetch_perms / new_with_perms(false)). The edge tables it crosses have no permissions (default NONE), but the predicate must still traverse them so that `task:t1` is visible to a workspace member. Conversely, a *direct* user-issued traversal of the same NONE edge stays gated, preserving the 3.1.x edge-permission security fix. Regression guard against the streaming executor over-blocking permission-clause traversals."

# The permission predicate walks the NONE edge tables unrestricted, so a
# workspace member can read the task it authorises.
[[test.results]]
value = "[task:t1]"

# A direct, user-issued traversal of the same NONE edge table is still gated:
# the member cannot enumerate the edge directly, even though the (trusted)
# permission predicate may walk it internally.
[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM task;
SELECT VALUE ->rel_user_member_of_workspace->workspace FROM ONLY user:martin;
