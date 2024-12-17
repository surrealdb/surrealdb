/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FOR select, create, update NONE' }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE FIELD test ON user PERMISSIONS FOR DELETE FULL, FOR SELECT, CREATE, UPDATE NONE;
INFO FOR TABLE user;
