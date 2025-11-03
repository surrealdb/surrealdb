/**
[env]
clean = true

[test]
reason = "Ensure a transaction fully rolls back if a unique constraint on 'name' is violated by a subsequent INSERT into 'item' within the same transaction. No data should persist."
issue = 5776
run = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database index `item_name_idx` already contains 'ItemA', with record `item:⟨item:1⟩`"

[[test.results]]
value = "[]"
*/


DEFINE INDEX item_name_idx ON TABLE item FIELDS name UNIQUE;

BEGIN;
INSERT INTO item (id, name) VALUES ('item:1', 'ItemA');
INSERT INTO item (id, name) VALUES ('item:2', 'ItemA');
COMMIT;

SELECT * FROM item;
