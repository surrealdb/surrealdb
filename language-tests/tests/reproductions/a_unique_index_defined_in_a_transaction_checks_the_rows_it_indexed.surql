/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A UNIQUE index defined by a transaction over rows committed before it checks
every value the transaction writes against the entries its build wrote,
which the transaction's snapshot does not show. A duplicate of a committed
value is a uniqueness error where it is written, not a write conflict at
COMMIT that a retry meets again. A row whose entry the build wrote cannot be
changed or deleted by the same transaction, so a swap of a value between two
rows is refused where it starts.
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
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database index `u` already contains 1, with record `item:1`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:3]"

[[test.results]]
error = "Database index `u` already contains 1, with record `item:1`"

[[test.results]]
error = "Database index `u` already contains 3, with record `item:3`"

[[test.results]]
value = "[item:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database index `u` already contains 5, with record `item:5`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `u` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[item:1, item:2, item:3, item:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, k: 1, seen: true }, { id: item:2, k: 2 }, { id: item:3, k: 3 }, { id: item:4, k: 4 }]"

[[test.results]]
error = "Database index `u` already contains 1, with record `item:1`"
*/

DEFINE TABLE item SCHEMALESS;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;

-- A value a committed row holds is refused when the transaction that defined
-- the index writes it again, and the transaction fails as a whole.
BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
CREATE item:3 SET k = 1 RETURN NONE;
COMMIT;
SELECT VALUE id FROM item ORDER BY id;
(INFO FOR TABLE item).indexes;

-- A value no row holds is written, and the index holds it once the
-- transaction commits, beside the entries of the committed rows.
BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
CREATE item:3 SET k = 3 RETURN NONE;
COMMIT;
SELECT VALUE id FROM item WITH INDEX u WHERE k = 3;
CREATE item:4 SET k = 1 RETURN NONE;
CREATE item:4 SET k = 3 RETURN NONE;
CREATE item:4 SET k = 4 RETURN VALUE id;
REMOVE INDEX u ON item;

-- Two rows the transaction writes with one new value meet the way they always do.
BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
CREATE item:5 SET k = 5 RETURN NONE;
CREATE item:6 SET k = 5 RETURN NONE;
COMMIT;

-- A row that holds a value cannot make way for another to take it within the
-- transaction that defined the index: its entry is the build's.
BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
DELETE item:1;
CREATE item:7 SET k = 1 RETURN NONE;
COMMIT;
SELECT VALUE id FROM item ORDER BY id;

-- An upsert that meets the value of a committed row lands on that row, as it
-- does once the definition has committed.
BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
UPSERT item SET k = 1, seen = true RETURN VALUE id;
COMMIT;
SELECT * FROM item ORDER BY id;
CREATE item:9 SET k = 1 RETURN NONE;
