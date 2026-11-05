/**
[env]
timeout = 5000
context-timeout = 5000

[test]
issue = 754
reason = "An index built concurrently can be overwritten and queried through its replacement"

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

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 2, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "[{ id: user:one }]"
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
CREATE user:two SET email = 'two@example.com', account = 'banana' RETURN NONE;
DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX test ON user).building.status != 'ready' { SLEEP 1s };
INFO FOR INDEX test ON user;
SELECT id FROM user WITH INDEX test WHERE account = 'apple';
