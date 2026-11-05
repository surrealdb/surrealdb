/**
[test]
reason = """
A blocking DEFINE INDEX run as its own statement commits its definition before
it waits for the build, so its transaction is never held open across the
build's writes, where a transaction aged past the history an engine retains
could no longer be validated at commit. This pins the contract the statement
keeps across that change: it returns with the index ready and populated, an
OVERWRITE returns with its replacement ready, a build that fails leaves no
index behind, and inside BEGIN ... CANCEL the definition rolls back with the
transaction.
"""
issue = 1205

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 100, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "[{ count: 100 }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 100, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `uniq` already contains 'same', with record `t:a`"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { ix: 'DEFINE INDEX ix ON t FIELDS v, id' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { ix: 'DEFINE INDEX ix ON t FIELDS v, id' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

*/

CREATE |t:100| SET v = rand::uuid() RETURN NONE;
DEFINE INDEX ix ON t FIELDS v;
INFO FOR INDEX ix ON t;
SELECT count() FROM t WITH INDEX ix WHERE v >= u'00000000-0000-0000-0000-000000000000' GROUP ALL;
DEFINE INDEX OVERWRITE ix ON t FIELDS v, id;
INFO FOR INDEX ix ON t;
CREATE t:a, t:b SET d = 'same', v = rand::uuid() RETURN NONE;
DEFINE INDEX uniq ON t FIELDS d UNIQUE;
INFO FOR TABLE t;
BEGIN;
DEFINE INDEX iy ON t FIELDS v;
CANCEL;
INFO FOR TABLE t;
CREATE t:c SET d = 'same', v = rand::uuid() RETURN NONE;
SELECT count() FROM t WHERE d = 'same' GROUP ALL;
