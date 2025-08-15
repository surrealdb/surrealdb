/**
[env]
timeout = 1500
clean = true

[test]

[[test.results]]
value = "[{ account: 'apple', email: 'test@surrealdb.com', id: user:1 }]"

[[test.results]]
value = "[{ account: 'tesla', email: 'test@surrealdb.com', id: user:2 }]"

[[test.results]]
value = "[{ account: 'apple', email: 'test@surrealdb.com', id: user:3 }]"

[[test.results]]
value = "[{ account: 'tesla', email: 'test@surrealdb.com', id: user:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The index 'test' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = '''{ building: { error: "Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`", status: 'error' } }'''


*/
CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
DEFINE INDEX test ON user FIELDS account, email UNIQUE CONCURRENTLY;
DEFINE INDEX test ON user COLUMNS account, email UNIQUE CONCURRENTLY;
SLEEP 500ms;
INFO FOR TABLE user;
INFO FOR INDEX test ON user;
