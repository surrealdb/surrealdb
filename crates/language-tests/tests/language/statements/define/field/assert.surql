/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z0-9]{3}/ PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ id: user:a, test: 'BBB' }]"

[[test.results]]
error = "Found 'aaa' for field `test`, with record `user:b`, but field must conform to: $value != NONE AND $value = /[A-Z0-9]{3}/"

[[test.results]]
error = "Found 111 for field `test`, with record `user:c`, but field must conform to: $value != NONE AND $value = /[A-Z0-9]{3}/"

[[test.results]]
error = "Found NONE for field `test`, with record `user:d`, but field must conform to: $value != NONE AND $value = /[A-Z0-9]{3}/"

*/
DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z0-9]{3}/;
REMOVE FIELD test ON user;
DEFINE FIELD test ON TABLE user ASSERT $value != NONE AND $value = /[A-Z0-9]{3}/;
INFO FOR TABLE user;

create user:a SET test = "BBB";
create user:b SET test = "aaa";
create user:c SET test = 111;
create user:d;
