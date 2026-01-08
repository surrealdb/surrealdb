/**
[test]

[[test.results]]
value = "[{ age: 23, id: user:1 }]"

[[test.results]]
value = "[{ age: 10, id: user:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS age' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ age: 24, id: user:1 }]"

[[test.results]]
value = "[{ age: 11, id: user:2 }]"

*/


CREATE user:1 SET age = 23;
CREATE user:2 SET age = 10;
DEFINE INDEX test ON user FIELDS age;
REMOVE INDEX test ON user;
DEFINE INDEX test ON user COLUMNS age;
INFO FOR TABLE user;
UPSERT user:1 SET age = 24;
UPSERT user:2 SET age = 11;
