/**
[env]
namespace = true
database = true

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = """
Issue 7404: `file::*` mutations are performed on the bucket's object store at once, so they
survive a cancelled transaction while the database writes beside them roll back.

This pins the behaviour as it is today. A bucket write inside a transaction is visible to the
rest of the query, so it cannot simply be deferred to commit; making it atomic with the
transaction is a design change, and this test flips when that lands."""
issue = 7404

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ f: f"b:/doc.txt", id: ref:1 }]'

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[f"b:/doc.txt"]'

[[test.results]]
value = "true"
*/

DEFINE BUCKET b BACKEND "memory";
CREATE ref:1 SET f = f"b:/doc.txt";
BEGIN;
file::put(f"b:/doc.txt", "payload");
UPDATE ref:1 SET f = f"b:/moved.txt";
CANCEL;
-- The record update rolled back; the bucket write did not.
SELECT VALUE f FROM ref:1;
f"b:/doc.txt".exists();
