/**
[env]
namespace = true
database = true

[test]
reason = "A table that DEFINE TABLE IF NOT EXISTS reports as defined is readable by the session's next statement, in a database the session's USE has just created. The issue is a race between sessions creating the same fresh database at once, which one session cannot express: it is covered by the kvs contract test `concurrent_creators_of_absent_key_commit_once` and the SDK test `define_table_if_not_exists_is_visible_under_concurrent_first_access`. This file pins the single-session sequence the race broke."
issue = 7553

[[test.results]]
value = "{ database: 'fresh_7553', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/
USE DB fresh_7553;
DEFINE TABLE IF NOT EXISTS t SCHEMALESS;
SELECT * FROM t:current;
DEFINE TABLE IF NOT EXISTS t SCHEMALESS;
SELECT * FROM t:current;
