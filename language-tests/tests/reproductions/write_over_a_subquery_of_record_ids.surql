/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write whose subjects are record ids a subquery yields writes the records they name, skipping an id whose record is missing, on every engine"

[[test.results]]
value = "[{ id: doc:1, n: 1 }]"

[[test.results]]
value = "[{ id: doc:2, n: 2 }]"

[[test.results]]
value = "[{ n: 1, open: 4 }, { n: 2, open: 4 }]"

[[test.results]]
value = "[{ n: 1, open: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 2, open: 7 }]"

[[test.results]]
value = "[{ id: doc:2, n: 2, open: 7 }]"

[[test.results]]
value = "[{ id: doc:1, n: 1, open: 5 }]"

*/

CREATE doc:1 SET n = 1;
CREATE doc:2 SET n = 2;
UPDATE (SELECT VALUE id FROM doc) SET open = 4 RETURN n, open;
UPDATE (SELECT VALUE id FROM doc WHERE n = 1) SET open = 5 RETURN n, open;
-- `doc:99` names no record, so there is nothing to update there.
UPDATE (SELECT VALUE doc:99 FROM doc WHERE n = 1) SET open = 6 RETURN n, open;
UPSERT (SELECT VALUE id FROM doc WHERE n = 2) SET open = 7 RETURN n, open;
DELETE (SELECT VALUE id FROM doc WHERE n = 2) RETURN BEFORE;
SELECT * FROM doc;
