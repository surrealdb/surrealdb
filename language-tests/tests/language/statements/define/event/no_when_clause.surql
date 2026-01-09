/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { some_event: 'DEFINE EVENT some_event ON user WHEN true THEN {  }' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE EVENT some_event ON TABLE user THEN {};
INFO FOR TABLE user;
