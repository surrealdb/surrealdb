/**
[env]
timeout = 30000
context-timeout = 30000
clean = true

[test]
reason = "DEFINE INDEX OVERWRITE that restates the definition of a ready index (the same table, columns and kind) keeps the index and its data, built or CONCURRENTLY, and refreshes only the comment: the build state still counts the records of the first build, not the one created after it. A changed definition, or a restated one over a failed index, rebuilds it."
issue = 7410

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
value = "{ initial: 2, status: 'ready' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ initial: 2, status: 'ready' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[2, "DEFINE INDEX ix ON t FIELDS v COMMENT 'second'"]"""

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ initial: 3, status: 'ready' }"

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
value = "'error'"

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
value = "{ initial: 1, status: 'ready' }"

*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET v = 1 RETURN NONE;
CREATE t:2 SET v = 2 RETURN NONE;
DEFINE INDEX ix ON t FIELDS v COMMENT 'first';
CREATE t:3 SET v = 3 RETURN NONE;
DEFINE INDEX OVERWRITE ix ON t FIELDS v COMMENT 'first';
RETURN (INFO FOR INDEX ix ON t).building.{ initial, status };
DEFINE INDEX OVERWRITE ix ON t FIELDS v COMMENT 'first' CONCURRENTLY;
RETURN (INFO FOR INDEX ix ON t).building.{ initial, status };
DEFINE INDEX OVERWRITE ix ON t FIELDS v COMMENT 'second';
RETURN [(INFO FOR INDEX ix ON t).building.initial, (INFO FOR TABLE t).indexes.ix];
SELECT VALUE id FROM t WITH INDEX ix WHERE v = 3;
DEFINE INDEX OVERWRITE ix ON t FIELDS v UNIQUE COMMENT 'second';
RETURN (INFO FOR INDEX ix ON t).building.{ initial, status };

DEFINE TABLE u SCHEMALESS;
CREATE u:1 SET e = 'x' RETURN NONE;
CREATE u:2 SET e = 'x' RETURN NONE;
DEFINE INDEX uq ON u FIELDS e UNIQUE CONCURRENTLY;
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'error' { SLEEP 1s };
RETURN (INFO FOR INDEX uq ON u).building.status;
DELETE u:2;
DEFINE INDEX OVERWRITE uq ON u FIELDS e UNIQUE CONCURRENTLY;
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX uq ON u).building.status != 'ready' { SLEEP 1s };
RETURN (INFO FOR INDEX uq ON u).building.{ initial, status };
