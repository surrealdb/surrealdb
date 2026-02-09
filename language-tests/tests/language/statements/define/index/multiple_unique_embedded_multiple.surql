/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { test: 'DEFINE INDEX test ON user FIELDS account, tags UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ account: 'apple', id: user:1, tags: ['one', 'two'] }]"

[[test.results]]
value = "[{ account: 'tesla', id: user:2, tags: ['one', 'two'] }]"

[[test.results]]
error = "Database index `test` already contains ['apple', 'two'], with record `user:1`"

[[test.results]]
error = "Database index `test` already contains ['tesla', 'two'], with record `user:2`"

*/
DEFINE INDEX test ON user FIELDS account, tags UNIQUE;
REMOVE INDEX test ON user;
DEFINE INDEX test ON user COLUMNS account, tags UNIQUE;
INFO FOR TABLE user;
CREATE user:1 SET account = 'apple', tags = ['one', 'two'];
CREATE user:2 SET account = 'tesla', tags = ['one', 'two'];
CREATE user:3 SET account = 'apple', tags = ['two', 'three'];
CREATE user:4 SET account = 'tesla', tags = ['two', 'three'];
