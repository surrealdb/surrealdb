/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FOR select NONE, FOR create, update FULL' }, indexes: {  }, lives: {  }, tables: {  } }"


*/
DEFINE FIELD test ON user PERMISSIONS FOR select NONE, FOR create, update FULL;
INFO FOR TABLE user;
