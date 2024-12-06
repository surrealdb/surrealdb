/**
[env]
# Extra timeout because this test has a sleep.
timeout = 2000

[test]

[[test.results]]
value = "[{ email: 'testA@surrealdb.com', id: user:1 }]"

[[test.results]]
value = "[{ email: 'testA@surrealdb.com', id: user:2 }]"

[[test.results]]
value = "[{ email: 'testB@surrealdb.com', id: user:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS email CONCURRENTLY' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ building: { status: 'built' } }"

[[test.results]]
value = "[{ detail: { plan: { index: 'test', operator: '=', value: 'testA@surrealdb.com' }, table: 'user' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ email: 'testA@surrealdb.com', id: user:1 }, { email: 'testA@surrealdb.com', id: user:2 }]"

[[test.results]]
value = "[{ email: 'testB@surrealdb.com', id: user:3 }]"
*/

CREATE user:1 SET email = 'testA@surrealdb.com';
CREATE user:2 SET email = 'testA@surrealdb.com';
CREATE user:3 SET email = 'testB@surrealdb.com';
DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
SLEEP 1s;
INFO FOR TABLE user;
INFO FOR INDEX test ON user;
SELECT * FROM user WHERE email = 'testA@surrealdb.com' EXPLAIN;
SELECT * FROM user WHERE email = 'testA@surrealdb.com';
SELECT * FROM user WHERE email = 'testB@surrealdb.com';
