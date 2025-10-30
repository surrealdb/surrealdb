/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FOR select FULL, FOR create, update NONE' }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE FIELD test ON user PERMISSIONS FOR SELECT FULL, FOR CREATE, UPDATE NONE;
INFO FOR TABLE user;
