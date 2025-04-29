/**
[env]
# Extra timeout because this test has a sleep.
timeout = 2000

[test]

[[test.results]]
value = "[{ email: 'testA@surrealdb.com', id: `user.csv`:1 }]"

[[test.results]]
value = "[{ email: 'testA@surrealdb.com', id: `user.csv`:2 }]"

[[test.results]]
value = "[{ email: 'testB@surrealdb.com', id: `user.csv`:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The index 'test' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON `user.csv` FIELDS email' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ building: { initial: 3, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "{ building: { initial: 3, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "[{ detail: { plan: { index: 'test', operator: '=', value: 'testA@surrealdb.com' }, table: 'user.csv' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ email: 'testA@surrealdb.com', id: `user.csv`:1 }, { email: 'testA@surrealdb.com', id: `user.csv`:2 }]"

[[test.results]]
value = "[{ email: 'testB@surrealdb.com', id: `user.csv`:3 }]"
*/

CREATE `user.csv`:1 SET email = 'testA@surrealdb.com';
CREATE `user.csv`:2 SET email = 'testA@surrealdb.com';
CREATE `user.csv`:3 SET email = 'testB@surrealdb.com';
DEFINE INDEX test ON `user.csv` FIELDS email CONCURRENTLY;
DEFINE INDEX `test` ON `user.csv` FIELDS email CONCURRENTLY;
SLEEP 1s;
INFO FOR TABLE `user.csv`;
INFO FOR INDEX test ON `user.csv`;
INFO FOR INDEX `test` ON `user.csv`;
SELECT * FROM `user.csv` WHERE email = 'testA@surrealdb.com' EXPLAIN;
SELECT * FROM `user.csv` WHERE email = 'testA@surrealdb.com';
SELECT * FROM `user.csv` WHERE email = 'testB@surrealdb.com';
