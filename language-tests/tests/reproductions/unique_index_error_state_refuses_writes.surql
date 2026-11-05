/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "A UNIQUE index whose CONCURRENTLY build failed on existing duplicates stayed defined in the error state, and writes skipped it: duplicates were accepted silently. A write that adds a key to such an index is refused until the index is rebuilt or removed. A write that adds no key is admitted, and a successful REBUILD restores enforcement."

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, id: t:2 }]"

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
match = '''$result.building.status == 'error''''

[[test.results]]
error = "Database index `ua` on table `t` failed to build, so it cannot enforce UNIQUE for this write: fix the duplicate values and run REBUILD INDEX, or REMOVE INDEX"

[[test.results]]
value = "[{ b: 1, id: t:4 }]"

[[test.results]]
error = "Database index `ua` on table `t` failed to build, so it cannot enforce UNIQUE for this write: fix the duplicate values and run REBUILD INDEX, or REMOVE INDEX"

[[test.results]]
value = "[{ a: 1, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `ua` already contains 1, with record `t:1`"

[[test.results]]
value = "[{ a: 2, id: t:6 }]"
*/
CREATE t:1 SET a = 1;
CREATE t:2 SET a = 1;
DEFINE INDEX ua ON t FIELDS a UNIQUE CONCURRENTLY;
-- Wait for the build to fail in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it end.
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
IF (INFO FOR INDEX ua ON t).building.status != 'error' { SLEEP 500ms };
INFO FOR INDEX ua ON t;
CREATE t:3 SET a = 2;
CREATE t:4 SET b = 1;
UPDATE t:4 SET a = 5;
DELETE t:2 RETURN BEFORE;
REBUILD INDEX ua ON t;
CREATE t:5 SET a = 1;
CREATE t:6 SET a = 2;
