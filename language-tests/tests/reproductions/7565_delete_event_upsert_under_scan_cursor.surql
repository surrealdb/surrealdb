/**
[env]
namespace = true
database = true

[test]
reason = "A DELETE scan carries on when a record's event rolls back a savepoint. The event upserts a shared row. For the second record that row already exists, so the UPSERT rolls back its create attempt while the DELETE's table-scan cursor is still open. Every record must still be visited, both events must run, and the transaction must commit."
issue = 7565

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1 }, { id: item:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: total:items, n: 2 }]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE TABLE total SCHEMALESS;
DEFINE FUNCTION fn::bump() {
	UPSERT total:items SET n = (n ?? 0) + 1;
};
DEFINE EVENT bump_on_delete ON item WHEN $event = 'DELETE' THEN {
	fn::bump();
};
CREATE item:1, item:2;
BEGIN;
DELETE item;
COMMIT;
SELECT * FROM item;
SELECT * FROM total;
