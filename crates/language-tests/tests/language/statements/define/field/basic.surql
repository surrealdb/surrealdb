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
[[test.results]]
value = "{ events: [ ], fields: [ { flex: false, name: 'test', permissions: { create: true, select: true, update: true }, readonly: false, what: 'user' } ], indexes: [ ], lives: [ ], tables: [ ] }"

*/
DEFINE FIELD test ON user;
REMOVE FIELD test ON user;
DEFINE FIELD test ON TABLE user;
INFO FOR TABLE user;
INFO FOR TABLE user STRUCTURE;

