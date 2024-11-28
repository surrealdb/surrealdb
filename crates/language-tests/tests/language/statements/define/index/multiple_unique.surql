/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The index 'test' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ account: 'apple', email: 'test@surrealdb.com', id: user:1 }]"

[[test.results]]
value = "[{ account: 'tesla', email: 'test@surrealdb.com', id: user:2 }]"

[[test.results]]
error = "Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"

[[test.results]]
error = "Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:2`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ account: 'apple', email: 'test@surrealdb.com', id: user:3 }]"

[[test.results]]
error = "Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:2`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ account: 'tesla', email: 'test@surrealdb.com', id: user:4 }]"

*/

DEFINE INDEX test ON user FIELDS account, email UNIQUE;
DEFINE INDEX test ON user COLUMNS account, email UNIQUE;
DEFINE INDEX IF NOT EXISTS test ON user COLUMNS account, email UNIQUE;
REMOVE INDEX test ON user;
DEFINE INDEX IF NOT EXISTS test ON user COLUMNS account, email UNIQUE;


INFO FOR TABLE user;
CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
DELETE user:1;
CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
DELETE user:2;
CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
