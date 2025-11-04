/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { test: 'DEFINE EVENT test ON user WHEN $before.email != $after.email THEN CREATE activity SET user = $this, `value` = $after.email, action = $event' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:test, updated_at: d'2024-11-28T14:57:41.914308226Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ email: 'info@surrealdb.com', id: user:test, updated_at: d'2024-11-28T14:57:41.917521775Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ email: 'test@surrealdb.com', id: user:test, updated_at: d'2024-11-28T14:57:41.918618086Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ count: 2 }]"

*/

DEFINE EVENT test ON user WHEN $before.email != $after.email THEN (
	CREATE activity SET user = $this, value = $after.email, action = $event
);
REMOVE EVENT test ON user;
DEFINE EVENT test ON TABLE user WHEN $before.email != $after.email THEN (
	CREATE activity SET user = $this, value = $after.email, action = $event
);
INFO FOR TABLE user;
UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now();
SELECT count() FROM activity GROUP ALL;
