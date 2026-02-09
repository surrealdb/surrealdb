/**
[env]
backend = ["rocksdb"]

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