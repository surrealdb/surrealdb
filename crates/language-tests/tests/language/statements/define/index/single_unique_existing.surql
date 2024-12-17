/**
[test]

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:1 }]"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:2 }]"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:3 }]"

[[test.results]]
error = "Database index `test` already contains 'test@surrealdb.com', with record `user:2`"

[[test.results]]
error = "Database index `test` already contains 'test@surrealdb.com', with record `user:2`"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/

CREATE user:1 SET email = 'info@surrealdb.com';
CREATE user:2 SET email = 'test@surrealdb.com';
CREATE user:3 SET email = 'test@surrealdb.com';
DEFINE INDEX test ON user FIELDS email UNIQUE;
DEFINE INDEX test ON user COLUMNS email UNIQUE;
INFO FOR TABLE user;
