/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "INSERT IGNORE skips only a duplicate record id or unique-index conflict: a field, VALUE or event failure is raised, and a rolled-back create attempt is not visible to a direct read later in the transaction"
issue = 7564

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field ``value`` of `item:1`: Expected `int` but found `'invalid'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event reject: An error occurred: rejected"

[[test.results]]
value = "{ item_count: 0, marker_count: 0 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:3, key: 'c', value: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ direct: [], indexed: [{ id: item:3, key: 'c', value: 1 }], scan: [{ id: item:3, key: 'c', value: 1 }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:3, key: 'c', value: 1 }]"
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD key ON item TYPE string;
DEFINE FIELD value ON item TYPE int;
DEFINE INDEX by_key ON item FIELDS key UNIQUE;

-- A field coercion failure is not a conflict, so it is raised
INSERT IGNORE INTO item { id: item:1, key: "a", value: "invalid" } RETURN AFTER;
SELECT * FROM item;

-- A CREATE event that throws is raised, and the function's earlier write
-- is rolled back with the rest of its transaction
DEFINE TABLE marker SCHEMALESS;
DEFINE EVENT reject ON item WHEN $event = "CREATE" THEN { THROW "rejected"; };
DEFINE FUNCTION fn::event_error() {
	CREATE marker:1 RETURN NONE;
	INSERT IGNORE INTO item { id: item:2, key: "b", value: 1 } RETURN NONE;
	RETURN true;
};
RETURN fn::event_error();
RETURN {
	marker_count: (SELECT count() FROM marker GROUP ALL)[0].count ?? 0,
	item_count: (SELECT count() FROM item GROUP ALL)[0].count ?? 0
};
REMOVE EVENT reject ON item;

-- A duplicate record id and a unique-key conflict are still skipped
CREATE item:3 SET key = "c", value = 1;
INSERT IGNORE INTO item { id: item:3, key: "z", value: 2 };
INSERT IGNORE INTO item { id: item:4, key: "c", value: 2 };

-- The skipped item:4 was written and rolled back: every read in the same
-- transaction agrees that it does not exist
BEGIN;
INSERT IGNORE INTO item { id: item:5, key: "c", value: 3 };
RETURN {
	direct: (SELECT * FROM item:5),
	indexed: (SELECT * FROM item WHERE key = "c"),
	scan: (SELECT * FROM item WITH NOINDEX)
};
COMMIT;

SELECT * FROM item;
