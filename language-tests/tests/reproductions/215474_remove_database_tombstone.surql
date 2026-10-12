/**
[env]
remote-exclusive = true
clean = true
namespace = true
database = false
auth = { level = "owner" }

[test]
reason = "REMOVE DATABASE removes the database immediately even though the data is reclaimed asynchronously in the background. The database disappears from the catalog at once, and recreating the same name yields a fresh, empty database with no data leaked from the removed one (Intercom 215474723434818, database-per-tenant deletion)."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'tenant', namespace: 'test' }"

[[test.results]]
value = "[{ id: thing:1, v: 1 }]"

[[test.results]]
value = "[{ id: thing:2, v: 2 }]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "{ database: 'home', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'tenant', namespace: 'test' }"

[[test.results]]
value = "[{ id: thing:9, v: 9 }]"

[[test.results]]
value = "[9]"
*/

-- A stable "home" database to operate from, plus the tenant we churn.
DEFINE DATABASE home;
DEFINE DATABASE tenant;

-- Populate the tenant database with some data.
USE DB tenant;
CREATE thing:1 SET v = 1;
CREATE thing:2 SET v = 2;
SELECT VALUE v FROM thing;

-- Switch away from the tenant, then remove it. REMOVE returns immediately; the
-- data prefix is reclaimed by a background task.
USE DB home;
REMOVE DATABASE tenant;

-- The tenant is no longer listed in the namespace catalog.
RETURN "tenant" IN object::keys((INFO FOR NS).databases);

-- Recreating the same name allocates a fresh internal database id, so the new
-- database is empty: only the newly created record is visible, none of the
-- removed database's records leak through.
DEFINE DATABASE tenant;
USE DB tenant;
CREATE thing:9 SET v = 9;
SELECT VALUE v FROM thing;
