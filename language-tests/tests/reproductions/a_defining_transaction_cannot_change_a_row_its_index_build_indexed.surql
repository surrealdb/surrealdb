/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A transaction that defined an index cannot change the indexed value of a row
the build indexed. The build wrote for every row of the table that existed,
whatever its values (an entry, a document mapping, a pending vector update),
none of it in the transaction's snapshot, so writing the same key fails the
commit against the build's own write however often the transaction is
retried. The change is refused where it is made, naming the record and the
index. A change that touches nothing the build wrote is made: another field,
a row the transaction created itself, and every change to a count index,
which keeps nothing of a row.
"""

[[test.results]]
value = "NONE"

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
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `by_k` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:3`: index `by_k` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `by_k` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `ft` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:3`: index `ft` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `ft` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[item:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `h` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:3`: index `h` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change record `item:1`: index `h` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:2, item:6]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 5 }]"
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE item SCHEMALESS;
CREATE item:1 SET k = 1, other = 1, name = 'alpha one', e = [1.0, 0.0] RETURN NONE;
CREATE item:2 SET k = 2, other = 1, name = 'beta two', e = [0.0, 1.0] RETURN NONE;
CREATE item:3 SET other = 1 RETURN NONE;

-- A B-tree index holds an entry for every row, the rows with no value included.
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
UPDATE item:1 SET k = 9 RETURN NONE;
COMMIT;
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
UPDATE item:3 SET k = 3 RETURN NONE;
COMMIT;
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
DELETE item:1;
COMMIT;
-- What leaves an entry of the build alone is written: another field, and the
-- rows the transaction creates, changes and deletes itself.
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
UPDATE item:1 SET other = 2 RETURN VALUE other;
CREATE item:4 SET k = 4 RETURN NONE;
UPDATE item:4 SET k = 5 RETURN NONE;
DELETE item:4;
COMMIT;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 4;
REMOVE INDEX by_k ON item;

-- A full-text index holds what its build wrote for every row, whether or not
-- there was text to analyse.
BEGIN;
DEFINE INDEX ft ON item FIELDS name FULLTEXT ANALYZER simple BM25;
UPDATE item:1 SET name = 'gamma' RETURN NONE;
COMMIT;
BEGIN;
DEFINE INDEX ft ON item FIELDS name FULLTEXT ANALYZER simple BM25;
UPDATE item:3 SET name = 'gamma three' RETURN NONE;
COMMIT;
BEGIN;
DEFINE INDEX ft ON item FIELDS name FULLTEXT ANALYZER simple BM25;
DELETE item:1;
COMMIT;
BEGIN;
DEFINE INDEX ft ON item FIELDS name FULLTEXT ANALYZER simple BM25;
UPDATE item:1 SET other = 3 RETURN NONE;
CREATE item:4 SET name = 'delta four' RETURN NONE;
UPDATE item:4 SET name = 'delta five' RETURN NONE;
CREATE item:5 SET name = 'epsilon six' RETURN NONE;
DELETE item:5;
COMMIT;
SELECT VALUE id FROM item WHERE name @@ 'alpha' ORDER BY id;
SELECT VALUE id FROM item WHERE name @@ 'five' ORDER BY id;
SELECT VALUE id FROM item WHERE name @@ 'four' ORDER BY id;
SELECT VALUE id FROM item WHERE name @@ 'epsilon' ORDER BY id;
REMOVE INDEX ft ON item;

-- So does a vector index.
BEGIN;
DEFINE INDEX h ON item FIELDS e HNSW DIMENSION 2 DIST EUCLIDEAN;
UPDATE item:1 SET e = [0.5, 0.5] RETURN NONE;
COMMIT;
BEGIN;
DEFINE INDEX h ON item FIELDS e HNSW DIMENSION 2 DIST EUCLIDEAN;
UPDATE item:3 SET e = [1.0, 1.0] RETURN NONE;
COMMIT;
BEGIN;
DEFINE INDEX h ON item FIELDS e HNSW DIMENSION 2 DIST EUCLIDEAN;
DELETE item:1;
COMMIT;
BEGIN;
DEFINE INDEX h ON item FIELDS e HNSW DIMENSION 2 DIST EUCLIDEAN;
UPDATE item:1 SET other = 4 RETURN NONE;
CREATE item:6 SET e = [1.0, 1.0] RETURN NONE;
UPDATE item:6 SET e = [0.9, 0.9] RETURN NONE;
CREATE item:7 SET e = [0.5, 0.5] RETURN NONE;
DELETE item:7;
COMMIT;
SELECT VALUE id FROM item WHERE e <|4,40|> [1.0, 1.0] ORDER BY id;
REMOVE INDEX h ON item;

-- A count index holds nothing of a row, so every change is written, and counted.
BEGIN;
DEFINE INDEX n ON item COUNT;
UPDATE item:1 SET k = 9 RETURN NONE;
DELETE item:2;
CREATE item:8 SET k = 8 RETURN NONE;
SELECT count() FROM item GROUP ALL;
COMMIT;
SELECT count() FROM item GROUP ALL;
