/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { test: 'DEFINE EVENT test ON user WHEN true THEN CREATE activity SET user = $parent.id, `value` = $after.email, action = $event, time = time::now()' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:test }]"

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:test }]"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:test }]"

[[test.results]]
value = "[{ action: 'CREATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'test@surrealdb.com' }]"


*/
DEFINE EVENT test ON user WHEN true THEN (
    CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
);
REMOVE EVENT test ON user;
DEFINE EVENT test ON TABLE user WHEN true THEN (
    CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
);
INFO FOR TABLE user;
UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now() RETURN id, email;
(SELECT * FROM activity ORDER BY time).{ action, user, value };
