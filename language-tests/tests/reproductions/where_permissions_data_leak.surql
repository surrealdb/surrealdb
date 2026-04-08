/**
[env]
imports = ["reproductions/where_permissions_data_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "accounts:1" }

[test]
reason = "WHERE clause must not execute before table-level permission checks, preventing data exfiltration via THROW or side effects"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

-- Table permissions deny SELECT, so THROW in WHERE must never execute.
-- Before the fix these would propagate the THROW error containing record data.
SELECT * FROM accounts WHERE THROW 'leaked: ' + secret;

-- Same for UPDATE WHERE
UPDATE accounts SET x = 1 WHERE THROW 'leaked: ' + secret;

-- Same for DELETE WHERE
DELETE accounts WHERE THROW 'leaked: ' + secret;

-- Same for UPSERT WHERE
UPSERT accounts SET x = 1 WHERE THROW 'leaked: ' + secret;
