/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE FIELD test ON user;
REMOVE FIELD test ON user;
DEFINE FIELD test ON TABLE user;
INFO FOR TABLE user;
