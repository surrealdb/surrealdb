/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 20000
context-timeout = 20000

[test]
reason = """
A transaction that defines a CONCURRENTLY index over rows committed before
it reads around the index until it commits, since the build reads nothing
before then and the index holds nothing. Nothing it writes is refused: its
writes are the build's to index once the transaction has closed, so the
index holds the rows as the transaction left them.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:3]"

[[test.results]]
value = "[item:1]"

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
value = "[item:3]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "'ready'"
*/

DEFINE TABLE item SCHEMALESS;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;

-- A CONCURRENTLY build reads nothing until the transaction has closed: the
-- transaction reads around the index meanwhile, writes what it likes, and the
-- build indexes what it wrote.
BEGIN;
DEFINE INDEX by_k ON item FIELDS k CONCURRENTLY;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
UPDATE item:1 SET k = 9 RETURN NONE;
DELETE item:2;
CREATE item:3 SET k = 1 RETURN NONE;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 9 ORDER BY id;
COMMIT;
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_k ON item).building.status != 'ready' { SLEEP 1s };
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 9 ORDER BY id;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 2 ORDER BY id;
(INFO FOR INDEX by_k ON item).building.status;
