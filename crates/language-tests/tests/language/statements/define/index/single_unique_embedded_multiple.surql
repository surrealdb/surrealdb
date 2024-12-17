/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS tags UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ id: user:1, tags: ['one', 'two'] }]"

[[test.results]]
error = "Database index `test` already contains 'two', with record `user:1`"

*/
DEFINE INDEX test ON user FIELDS tags UNIQUE;
REMOVE INDEX test ON user;
DEFINE INDEX test ON user COLUMNS tags UNIQUE;
INFO FOR TABLE user;
CREATE user:1 SET tags = ['one', 'two'];
CREATE user:2 SET tags = ['two', 'three'];
