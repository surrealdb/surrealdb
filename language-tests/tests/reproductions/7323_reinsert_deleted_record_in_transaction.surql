/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A transaction can delete a record and insert the same id again"
issue = 7323

[[test.results]]
value = "[{ id: no_tx7323:1 }]"

[[test.results]]
value = "[{ id: no_tx7323:1 }]"

[[test.results]]
value = "[{ id: no_tx7323:1 }]"

[[test.results]]
value = "[{ id: no_tx7323:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tx7323:1 }]"

[[test.results]]
value = "[{ id: tx7323:1 }]"

[[test.results]]
value = "[{ id: tx7323:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: rel_tx7323:1, in: in_tx7323:1, out: out_tx7323:1 }]"

[[test.results]]
value = "[{ id: rel_tx7323:1, in: in_tx7323:1, out: out_tx7323:1 }]"

[[test.results]]
value = "[{ id: rel_tx7323:1, in: in_tx7323:1, out: out_tx7323:1 }]"

[[test.results]]
value = "NONE"
*/

INSERT INTO no_tx7323 { id: 1 };
DELETE no_tx7323:1 RETURN BEFORE;
INSERT INTO no_tx7323 { id: 1 };
DELETE no_tx7323:1 RETURN BEFORE;

BEGIN;
INSERT INTO tx7323 { id: 1 };
DELETE tx7323:1 RETURN BEFORE;
INSERT INTO tx7323 { id: 1 };
COMMIT;

DEFINE TABLE OVERWRITE rel_tx7323 TYPE RELATION;

BEGIN;
RELATE in_tx7323:1 -> rel_tx7323:1 -> out_tx7323:1;
DELETE rel_tx7323:1 RETURN BEFORE;
RELATE in_tx7323:1 -> rel_tx7323:1 -> out_tx7323:1;
COMMIT;
