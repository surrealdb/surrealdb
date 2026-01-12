/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ account: 'apple', email: 'test@surrealdb.com', id: user:1 }]"

[[test.results]]
value = "[{ account: 'tesla', email: 'test@surrealdb.com', id: user:2 }]"

[[test.results]]
value = "[{ account: 'apple', email: 'test@surrealdb.com', id: user:3 }]"

[[test.results]]
value = "[{ account: 'tesla', email: 'test@surrealdb.com', id: user:4 }]"

*/
DEFINE INDEX test ON user FIELDS account, email;
REMOVE INDEX test ON user;
DEFINE INDEX test ON user COLUMNS account, email;
INFO FOR TABLE user;
CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
