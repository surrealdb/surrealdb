/**
[env]
timeout = 5000
context-timeout = 5000

[test]
issue = 754
reason = "An index built concurrently can be overwritten and queried through its replacement"

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
value = "{ building: { initial: 2, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "[{ id: user:one }]"
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
CREATE user:two SET email = 'two@example.com', account = 'banana' RETURN NONE;
DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
SLEEP 1s;
DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY;
SLEEP 1s;
INFO FOR INDEX test ON user;
SELECT id FROM user WITH INDEX test WHERE account = 'apple';
