/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ id: user:a, test: 'a' }]"

[[test.results]]
error = "Couldn't coerce value for field `test` of `user:b`: Expected `string` but found `1`"

*/
DEFINE FIELD test ON user TYPE string;
REMOVE FIELD test ON user;
DEFINE FIELD test ON TABLE user TYPE string;
INFO FOR TABLE user;

CREATE user:a SET test = "a";
CREATE user:b SET test = 1;
