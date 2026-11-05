/**
[env]
namespace = true
database = true

[test]
reason = "Scanning a table after all its records were deleted visits no deleted rows, so repeated create/delete cycles leave an empty scan empty. Tombstone accumulation is a RocksDB property. This pins the result, not scan time."
issue = 5324

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE caaa SCHEMAFULL;
DEFINE FIELD a ON caaa TYPE string;
FOR $round IN 0..3 { CREATE |caaa:500| CONTENT { a: rand::string(50, 300) } RETURN NONE; DELETE caaa RETURN NONE; };
SELECT * FROM caaa LIMIT 0;
SELECT * FROM caaa;
SELECT count() FROM caaa GROUP ALL;
