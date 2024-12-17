/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE FIELD test ON user PERMISSIONS FOR delete NONE, FOR select, create, update FULL;
INFO FOR TABLE user;
