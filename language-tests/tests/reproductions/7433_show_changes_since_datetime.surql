/**
[env]
namespace = true
database = true
backend = ["mem", "rocksdb", "surrealkv"]

[test]
reason = "SHOW CHANGES with a datetime SINCE returned an empty result on mem and surrealkv: the datetime was converted using the backend's versioned-storage timestamp domain (nanoseconds since epoch) while changefeed keys are stamped with the commit timestamp oracle (HLC), so the scan lower bound landed past every stored entry. A datetime SINCE below the first change must return the same feed as the versionstamp form."
issue = 7433

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cf:1, x: 1 }]"

[[test.results]]
value = "[{ id: cf:1, x: 2 }]"

[[test.results]]
match = "type::is_array($result) && count($result) >= 2"

[[test.results]]
match = "type::is_array($result) && count($result) >= 2"

*/

DEFINE TABLE cf SCHEMALESS CHANGEFEED 1d;
CREATE cf:1 SET x = 1;
UPDATE cf:1 SET x = 2;
-- Versionstamps are non-deterministic, so only assert the changeset count.
SHOW CHANGES FOR TABLE cf SINCE 0 LIMIT 100;
-- The datetime form must see the same changes (was: always []).
SHOW CHANGES FOR TABLE cf SINCE d'2020-01-01T00:00:00Z' LIMIT 100;
