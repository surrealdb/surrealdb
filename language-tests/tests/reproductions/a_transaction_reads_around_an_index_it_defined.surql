/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A transaction that defines an index over rows committed before it answers
its reads without that index. A blocking build writes the entries of those
rows in transactions of its own, which the defining transaction's snapshot
does not show, so a read through the index would answer for the rows the
transaction wrote and for none of the others. The index serves every read
once the transaction commits, entries of the rows it wrote included.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:3]"

[[test.results]]
value = "[item:1, item:3]"

[[test.results]]
value = "[item:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1, item:3, item:4]"

[[test.results]]
value = "[item:1, item:3, item:4, item:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:3, item:4]"

[[test.results]]
value = "[item:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:3, item:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1, item:3, item:4, item:6]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:3, item:4, item:6]"
*/

DEFINE TABLE item SCHEMALESS;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;
CREATE item:3 SET k = 1 RETURN NONE;

-- A B-tree index is read around: the rows committed before the definition
-- answer, and the rows the transaction writes after it, once it commits too.
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM item WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM item WHERE k > 1 ORDER BY id;
CREATE item:4 SET k = 1 RETURN NONE;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM item ORDER BY k, id;
COMMIT;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM item WHERE k > 1 ORDER BY id;

-- A count index is read around too: it counts what the build counted, which
-- the transaction cannot see.
BEGIN;
DEFINE INDEX n ON item COUNT;
SELECT count() FROM item GROUP ALL;
CREATE item:5 SET k = 5 RETURN NONE;
SELECT count() FROM item GROUP ALL;
COMMIT;
SELECT count() FROM item GROUP ALL;

-- A replacement is read around as a new index is: the index it replaces has
-- entries the replacement's build rewrote.
BEGIN;
DEFINE INDEX OVERWRITE by_k ON item FIELDS k;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
CREATE item:6 SET k = 1 RETURN NONE;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
COMMIT;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
