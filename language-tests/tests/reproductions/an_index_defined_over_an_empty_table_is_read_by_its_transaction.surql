/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index a transaction defines over a table the store holds no record of is
built with nothing to write, so there is no entry the transaction cannot
read and no write that fails its commit. The transaction reads the index,
and creates, changes and deletes the rows it holds, as it would any index.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

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
value = "[item:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:2]"

[[test.results]]
error = "Database index `u` already contains 2, with record `item:2`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database index `u` already contains 1, with record `item:1`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

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
value = "NONE"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

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
value = "[item:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1]"
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE item SCHEMALESS;

-- An index defined over a table with no rows is built with nothing to write,
-- so its transaction reads it, and changes every row it holds, as it would
-- any index.
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
CREATE item:1 SET k = 1 RETURN NONE;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1;
UPDATE item:1 SET k = 2 RETURN NONE;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 2;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1;
COMMIT;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 2;
REMOVE INDEX by_k ON item;
DELETE item;

BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
CREATE item:1 SET k = 1 RETURN NONE;
UPDATE item:1 SET k = 2 RETURN NONE;
DELETE item:1;
CREATE item:2 SET k = 2 RETURN NONE;
SELECT VALUE id FROM item WITH INDEX u WHERE k = 2;
COMMIT;
SELECT VALUE id FROM item ORDER BY id;
CREATE item:3 SET k = 2 RETURN NONE;
REMOVE INDEX u ON item;
DELETE item;

BEGIN;
DEFINE INDEX u ON item FIELDS k UNIQUE;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 1 RETURN NONE;
COMMIT;
SELECT VALUE id FROM item ORDER BY id;
DELETE item;

BEGIN;
DEFINE INDEX n ON item COUNT;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;
DELETE item:1;
SELECT count() FROM item GROUP ALL;
COMMIT;
SELECT count() FROM item GROUP ALL;
REMOVE INDEX n ON item;
DELETE item;

BEGIN;
DEFINE INDEX ft ON item FIELDS name FULLTEXT ANALYZER simple BM25;
CREATE item:1 SET name = 'alpha one' RETURN NONE;
CREATE item:2 SET name = 'beta two' RETURN NONE;
UPDATE item:1 SET name = 'gamma three' RETURN NONE;
DELETE item:2;
COMMIT;
SELECT VALUE id FROM item WHERE name @@ 'gamma' ORDER BY id;
SELECT VALUE id FROM item WHERE name @@ 'alpha' ORDER BY id;
REMOVE INDEX ft ON item;
DELETE item;

BEGIN;
DEFINE INDEX h ON item FIELDS e HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE item:1 SET e = [1.0, 0.0] RETURN NONE;
CREATE item:2 SET e = [0.0, 1.0] RETURN NONE;
UPDATE item:1 SET e = [0.5, 0.5] RETURN NONE;
DELETE item:2;
SELECT VALUE id FROM item WHERE e <|2,40|> [1.0, 1.0] ORDER BY id;
COMMIT;
SELECT VALUE id FROM item WHERE e <|2,40|> [1.0, 1.0] ORDER BY id;
