/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { test: 'DEFINE EVENT test ON user ASYNC RETRY 1 MAXDEPTH 3 WHEN true THEN (CREATE activity SET user = $parent.id, `value` = $after.email, action = $event, time = time::now())' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { test: 'DEFINE EVENT test ON user ASYNC RETRY 2 MAXDEPTH 6 WHEN true THEN (CREATE activity SET user = $parent.id, `value` = $after.email, action = $event, time = time::now())' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE EVENT test ON user ASYNC WHEN true THEN (
    CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
);
INFO FOR TABLE user;
REMOVE EVENT test ON user;
DEFINE EVENT test ON TABLE user ASYNC RETRY 2 MAXDEPTH 6 WHEN true THEN (
    CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
);
INFO FOR TABLE user;

