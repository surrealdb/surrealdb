/**
[env]
# Extra timeout because this test sleeps.
timeout = 10000
context-timeout = 10000

[test]
reason = "An index build started inside a transaction sees what that transaction defined: a blocking full-text build analyses with the analyzer defined beside it, and a CONCURRENTLY build reads nothing before the commit, so it sees the analyzer, the records written before its definition, and a PREPARE REMOVE committed with it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:needle }]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
# `compacting` is left out: the build's full-text writes wait for the background
# compaction task, which a server runs and the embedded harness does not.
match = "$result.building.{ initial, pending, status, updated } == { initial: 3, pending: 0, status: 'ready', updated: 0 }"

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, error, initial, pending, status } == { armed_shards: 0, compacting: false, error: 'Index building has been cancelled: Prepare remove.', initial: 0, pending: 0, status: 'error' }"

[[test.results]]
value = "[{ id: user:added }]"

[[test.results]]
value = "[{ id: user:changed }]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:needle SET email = 'needle@example.com' RETURN NONE;
CREATE user:changed SET email = 'before@example.com' RETURN NONE;

-- A blocking build analyses with the analyzer its transaction defines.
BEGIN;
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blocking ON user FIELDS email FULLTEXT ANALYZER simple BM25;
COMMIT;
SELECT id FROM user WITH INDEX blocking WHERE email @@ 'needle';

-- A CONCURRENTLY build waits for its transaction to commit, however long the
-- transaction stays open after defining it.
BEGIN;
DEFINE ANALYZER plain TOKENIZERS blank,class;
CREATE user:added SET email = 'added@example.com' RETURN NONE;
UPDATE user:changed SET email = 'after@example.com' RETURN NONE;
DEFINE INDEX concurrent ON user FIELDS email FULLTEXT ANALYZER plain BM25 CONCURRENTLY;
DEFINE INDEX decommissioned ON user FIELDS email CONCURRENTLY;
SLEEP 500ms;
ALTER INDEX decommissioned ON user PREPARE REMOVE;
COMMIT;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX concurrent ON user).building.status != 'ready' { SLEEP 1s };
INFO FOR INDEX concurrent ON user;
INFO FOR INDEX decommissioned ON user;
SELECT id FROM user WITH INDEX concurrent WHERE email @@ 'added';
SELECT id FROM user WITH INDEX concurrent WHERE email @@ 'after';
SELECT id FROM user WITH INDEX concurrent WHERE email @@ 'before';
