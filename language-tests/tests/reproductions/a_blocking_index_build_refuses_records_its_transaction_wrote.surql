/**
[test]
requires = ["scripting"]
reason = "A DEFINE INDEX without CONCURRENTLY builds from the table as last committed. Built in a transaction that had written records of the table, it missed those records and kept entries for the values they gave up, which a UNIQUE index then refused to every other record. Such a definition is refused, whether the transaction created the table's records, changed them or deleted them, and whether a statement or an embedded script defines the index; the transaction keeps nothing it wrote, and once the writes are committed the same definition indexes them."

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
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot define index `email` on `user`: this transaction has written records of `user`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "['before@example.com', 'dup@example.com']"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot define index `email` on `fresh`: this transaction has written records of `fresh`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
error = "The table 'fresh' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = """
Problem with embedded script function. An exception occurred: Cannot define index `email` on `user`: this transaction has written records of `user`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first.
"""

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "['before@example.com', 'dup@example.com']"

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
value = "[{ id: user:added }]"

[[test.results]]
value = "[{ id: user:changed }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['dup@example.com']"
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:moved SET email = 'dup@example.com' RETURN NONE;
CREATE user:changed SET email = 'before@example.com' RETURN NONE;

-- The transaction writes the table, then defines an index on it.
BEGIN;
CREATE user:added SET email = 'added@example.com' RETURN NONE;
UPDATE user:changed SET email = 'after@example.com' RETURN NONE;
UPDATE user:moved SET email = 'moved@example.com' RETURN NONE;
DEFINE INDEX email ON user FIELDS email UNIQUE;
COMMIT;
SELECT VALUE email FROM user ORDER BY email;
INFO FOR TABLE user;

-- A table whose every record the transaction created.
BEGIN;
CREATE fresh:1 SET email = 'fresh@example.com' RETURN NONE;
DEFINE INDEX email ON fresh FIELDS email;
COMMIT;
SELECT * FROM fresh;

-- A definition an embedded script runs, after a delete.
BEGIN;
DELETE user:changed;
(function() { return surrealdb.query("DEFINE INDEX email ON user FIELDS email UNIQUE") });
COMMIT;
SELECT VALUE email FROM user ORDER BY email;

-- Committed first, the writes are what the build reads.
BEGIN;
CREATE user:added SET email = 'added@example.com' RETURN NONE;
UPDATE user:changed SET email = 'after@example.com' RETURN NONE;
UPDATE user:moved SET email = 'moved@example.com' RETURN NONE;
COMMIT;
DEFINE INDEX email ON user FIELDS email UNIQUE;
SELECT id FROM user WITH INDEX email WHERE email = 'added@example.com';
SELECT id FROM user WITH INDEX email WHERE email = 'after@example.com';
SELECT id FROM user WITH INDEX email WHERE email = 'before@example.com';
-- The value `user:moved` gave up is free.
CREATE user:taker SET email = 'dup@example.com' RETURN VALUE email;
