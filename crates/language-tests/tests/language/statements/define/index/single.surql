/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS email' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:1 }]"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:2 }]"

*/

DEFINE INDEX test ON user FIELDS email;
REMOVE INDEX test ON user;
DEFINE INDEX test ON user COLUMNS email;
INFO FOR TABLE user;
CREATE user:1 SET email = 'test@surrealdb.com';
CREATE user:2 SET email = 'test@surrealdb.com';
