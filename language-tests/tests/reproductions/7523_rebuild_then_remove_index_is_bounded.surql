/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7523 (stranded 'cleaning' on TiKV 3.2.4, no recipe to enter it): on main a REBUILD then REMOVE of an index completes at once and the table keeps accepting indexed writes. The stranded-state entry could not be reproduced here."
issue = 7523

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
value = "[]"

[[test.results]]
value = "[{ count: 501 }]"
*/

DEFINE TABLE ioc SCHEMALESS;
FOR $i IN 1..=500 { CREATE type::record("ioc", $i) SET net = 'x' + <string> $i RETURN NONE; };
DEFINE INDEX ix ON ioc FIELDS net CONCURRENTLY;
-- Wait for the build in bounded steps, each its own statement: the reads of a
-- single statement all come from one snapshot and would never see it finish.
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX ix ON ioc).building.status != 'ready' { SLEEP 1s };
REBUILD INDEX ix ON ioc;
REMOVE INDEX ix ON ioc;
CREATE ioc:new SET net = 'fresh' RETURN NONE;
SELECT count() FROM ioc GROUP ALL;
