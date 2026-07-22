/**
[env]
backend = ["mem"]

[test]

[[test.results]]
error = "There was a problem with the key-value store: The storage layer does not support compaction requests."

[[test.results]]
error = "There was a problem with the key-value store: The storage layer does not support compaction requests."

[[test.results]]
error = "There was a problem with the key-value store: The storage layer does not support compaction requests."

[[test.results]]
value = '[{ id: test:1 }]'

[[test.results]]
error = "There was a problem with the key-value store: The storage layer does not support compaction requests."

*/

ALTER SYSTEM COMPACT;
ALTER NAMESPACE COMPACT;
ALTER DATABASE COMPACT;
CREATE test:1;
ALTER TABLE test COMPACT;

