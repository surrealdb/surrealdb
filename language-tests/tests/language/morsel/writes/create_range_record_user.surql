/**
[env]
imports = ["language/morsel/writes/create_range_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }
timeout = 30000

[test]
reason = "a record user's CREATE over a range of ids is judged by the create permission of the table for each id, and leaves what the serial run leaves, whatever width the compute half of the statement runs at"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1499 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 299 }]"

[[test.results]]
error = "Database record `some:1201` already exists"

[[test.results]]
value = "[{ count: 299 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"
*/

CREATE |open:1..1500| RETURN NONE;
SELECT count() FROM open GROUP ALL;
CREATE |some:1..1500| RETURN NONE;
SELECT count() FROM some GROUP ALL;
CREATE |some:1201..1500| RETURN NONE;
SELECT count() FROM some GROUP ALL;
CREATE |shut:1..1500| RETURN NONE;
SELECT count() FROM shut GROUP ALL;
