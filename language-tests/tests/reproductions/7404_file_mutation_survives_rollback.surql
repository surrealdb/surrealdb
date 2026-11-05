/**
[env]
namespace = true
database = true

[test]
reason = """
Issue 7404: file writes are not transactional. `file::put`, `file::delete` and `file::rename`
act on the bucket when they run, and a cancelled transaction does not undo them, while the
records written beside them roll back."""
issue = 7404

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[f"b:/doc.txt"]'

[[test.results]]
value = "['/moved.txt', '/new.txt']"
*/

DEFINE BUCKET b BACKEND "memory";
file::put(f"b:/doc.txt", "original");
file::put(f"b:/old.txt", "old");
CREATE ref:1 SET f = f"b:/doc.txt";

BEGIN;
file::put(f"b:/new.txt", "new");
f"b:/old.txt".delete();
f"b:/doc.txt".rename("moved.txt");
UPDATE ref:1 SET f = f"b:/moved.txt";
CANCEL;

-- The record rolled back; every bucket write stands.
SELECT VALUE f FROM ref:1;
file::list("b").map(|$o| file::key($o.file));
