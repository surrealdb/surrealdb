/**
[env]
# Extra timeout because this test sleeps.
timeout = 10000

[test]
reason = "A transaction that builds an index inside itself holds back the writes other transactions make to that table until it ends, since they cannot see the index and the build reads before it commits. The interleaving needs two transactions open at once, which one session cannot hold; `surrealdb/core/src/kvs/index/tests.rs` drives it. This file holds what the hold must leave alone: the defining transaction's own writes of the table, on either side of the definition; a transaction that defines a CONCURRENTLY and a blocking index on one table, whose write after both is in both; and the UNIQUE index refusing a second record that holds a value the transaction wrote."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

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

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 1 }"

[[test.results]]
value = "[{ id: user:early }]"

[[test.results]]
value = "[{ id: user:late }]"

[[test.results]]
error = "Database index `email` already contains 'late@example.com', with record `user:late`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:late }]"

[[test.results]]
error = "Database index `email` already contains 'late@example.com', with record `account:late`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: book:late }]"

[[test.results]]
value = "[{ id: book:late }]"

[[test.results]]
error = "Database index `isbn` already contains 'late', with record `book:late`"
*/

DEFINE TABLE user SCHEMALESS;
DEFINE TABLE account SCHEMALESS;
CREATE user:seed SET email = 'seed@example.com' RETURN NONE;
CREATE account:seed SET email = 'seed@example.com' RETURN NONE;

-- A CONCURRENTLY definition between two writes of its table, in one
-- transaction: the build reads once the transaction has committed, so it reads
-- both writes.
BEGIN;
CREATE user:early SET email = 'early@example.com' RETURN NONE;
DEFINE INDEX email ON user FIELDS email UNIQUE CONCURRENTLY;
CREATE user:late SET email = 'late@example.com' RETURN NONE;
COMMIT;
SLEEP 1s;
INFO FOR INDEX email ON user;
SELECT id FROM user WITH INDEX email WHERE email = 'early@example.com';
SELECT id FROM user WITH INDEX email WHERE email = 'late@example.com';
CREATE user:taken SET email = 'late@example.com' RETURN NONE;

-- A blocking definition followed by a write of its table, in one transaction:
-- the write sees the index and maintains it.
BEGIN;
DEFINE INDEX email ON account FIELDS email UNIQUE;
CREATE account:late SET email = 'late@example.com' RETURN NONE;
COMMIT;
SELECT id FROM account WITH INDEX email WHERE email = 'late@example.com';
CREATE account:taken SET email = 'late@example.com' RETURN NONE;

-- A CONCURRENTLY and a blocking definition on one table, in one transaction,
-- with a write after both: the blocking build reads before the transaction
-- commits and the other once it has, and the write is indexed by both.
DEFINE TABLE book SCHEMALESS;
CREATE book:seed SET isbn = 'seed' RETURN NONE;
BEGIN;
DEFINE INDEX other ON book FIELDS isbn CONCURRENTLY;
DEFINE INDEX isbn ON book FIELDS isbn UNIQUE;
CREATE book:late SET isbn = 'late' RETURN NONE;
COMMIT;
SLEEP 1s;
SELECT id FROM book WITH INDEX other WHERE isbn = 'late';
SELECT id FROM book WITH INDEX isbn WHERE isbn = 'late';
CREATE book:taken SET isbn = 'late' RETURN NONE;
