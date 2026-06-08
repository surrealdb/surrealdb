/**
[env]
namespace = true
database = true
auth = { level = "viewer" }

[test]
reason = "USE must not implicitly create namespaces or databases on behalf of a session that lacks DEFINE NAMESPACE / DEFINE DATABASE authorization. The session can still set its context (so SDKs that issue USE before SIGNIN keep working), but `tx.get_or_add_ns` / `tx.ensure_ns_db` only runs when the caller has the same `Edit` authorization that `DEFINE NAMESPACE` / `DEFINE DATABASE` enforce. Without that authorization the session lands on a target the catalog never created, and the next query that needs the namespace surfaces `NsNotFound` / `DbNotFound` — the same security outcome as the older `NotAllowed`, without the side-effect of materialising resources the session was never allowed to materialise."

# 1: A Root viewer can switch into the pre-provisioned test ns/db.
[[test.results]]
value = "{ database: 'test', namespace: 'test' }"

# 2: Switching to a brand-new namespace returns the requested context
# but does NOT create the namespace. The session retains the previously
# selected database while only the namespace is updated.
[[test.results]]
value = "{ database: 'test', namespace: 'new_namespace_auth' }"

# 3: Switching to a brand-new database under an existing namespace
# behaves the same way — context updated, but `new_database_auth` is
# never materialised.
[[test.results]]
value = "{ database: 'new_database_auth', namespace: 'test' }"

# 4: Re-selecting `new_namespace_auth` keeps the previously selected
# database; this exercises the same code path used by SDKs that send
# `use` before authenticating, and we still must not auto-create.
[[test.results]]
value = "{ database: 'new_database_auth', namespace: 'new_namespace_auth' }"

# 5: A query under the unprovisioned namespace must surface NsNotFound,
# proving the USE in query 2 / 4 did NOT auto-create the namespace.
[[test.results]]
error = "The namespace 'new_namespace_auth' does not exist"

# 6: Switch back into the existing test namespace with the still-unset
# `new_database_auth`.
[[test.results]]
value = "{ database: 'new_database_auth', namespace: 'test' }"

# 7: A query under the unprovisioned database must surface DbNotFound,
# proving the USE in query 3 / 6 did NOT auto-create the database.
[[test.results]]
error = "The database 'new_database_auth' does not exist"
*/

USE NS test DB test;
USE NS new_namespace_auth;
USE NS test DB new_database_auth;
USE NS new_namespace_auth;
SELECT * FROM thing;
USE NS test DB new_database_auth;
SELECT * FROM thing;
