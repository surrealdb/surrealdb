/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { test: 'DEFINE EVENT test ON user WHEN true THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:test, updated_at: d'2024-10-29T16:18:39.540699827Z' }]"

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:test, updated_at: d'2024-10-30T16:18:39.540699827Z' }]"

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:test, updated_at: d'2024-10-29T16:18:39.540699827Z' }]"

[[test.results]]
value = "[{ count: 3 }]"

*/
DEFINE EVENT test ON user WHEN true THEN (
    CREATE activity SET user = $this, value = $after.email, action = $event
);
REMOVE EVENT test ON user;
DEFINE EVENT test ON TABLE user WHEN true THEN (
    CREATE activity SET user = $this, value = $after.email, action = $event
);
INFO FOR TABLE user;
UPSERT user:test SET email = 'info@surrealdb.com', updated_at = d'2024-10-29T16:18:39.540699827Z';
UPSERT user:test SET email = 'info@surrealdb.com', updated_at = d'2024-10-30T16:18:39.540699827Z';
UPSERT user:test SET email = 'test@surrealdb.com', updated_at = d'2024-10-29T16:18:39.540699827Z';
SELECT count() FROM activity GROUP ALL;
