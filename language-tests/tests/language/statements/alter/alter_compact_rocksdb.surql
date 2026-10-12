/**
[env]
remote-exclusive = true
backend = ["rocksdb"]
# `ALTER SYSTEM COMPACT` compacts the whole store, so against a server that has
# served the rest of the suite it is doing real work; the default 1s budget
# describes a query, not this.
timeout = 30000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ id: test:1 }]'

[[test.results]]
value = "NONE"

*/

ALTER SYSTEM COMPACT;
ALTER NAMESPACE COMPACT;
ALTER DATABASE COMPACT;
CREATE test:1;
ALTER TABLE test COMPACT