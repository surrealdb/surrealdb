/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { custom: 'DEFINE FIELD custom ON user TYPE array PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD custom ON user TYPE array<any>;
INFO FOR TABLE user;
