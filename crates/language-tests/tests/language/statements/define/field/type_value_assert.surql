/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ events: {  }, fields: { test: "DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z0-9]{3}/ PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }'''

[[test.results]]
error = "Found NONE for field `test`, with record `user:a`, but expected a string"

[[test.results]]
value = "[{ id: user:b, test: '111' }]"

[[test.results]]
error = "Found 111 for field `test`, with record `user:c`, but expected a string"

[[test.results]]
value = "[{ id: user:d, test: 'AAA' }]"

[[test.results]]
error = "Found 'AA' for field `test`, with record `user:e`, but field must conform to: $value != NONE AND $value = /[A-Z0-9]{3}/"

*/
DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z0-9]{3}/;
REMOVE FIELD test ON user;
DEFINE FIELD test ON TABLE user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z0-9]{3}/;
INFO FOR TABLE user;

create user:a;
create user:b SET test = "111";
create user:c SET test = 111;
create user:d SET test = "AAA";
create user:e SET test = "AA";
