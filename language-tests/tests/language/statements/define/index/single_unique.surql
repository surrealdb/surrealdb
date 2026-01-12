/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS email UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:1 }]"

[[test.results]]
error = "Database index `test` already contains 'test@surrealdb.com', with record `user:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:2 }]"

*/
DEFINE INDEX test ON user FIELDS email UNIQUE;
REMOVE INDEX test ON user;
DEFINE INDEX test ON user COLUMNS email UNIQUE;
INFO FOR TABLE user;
CREATE user:1 SET email = 'test@surrealdb.com';
CREATE user:2 SET email = 'test@surrealdb.com';
DELETE user:1;
CREATE user:2 SET email = 'test@surrealdb.com';
