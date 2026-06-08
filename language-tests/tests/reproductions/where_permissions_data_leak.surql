/**
[env]
imports = ["reproductions/where_permissions_data_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "accounts:1" }

[test]
reason = "User-supplied expressions (WHERE, SET, MERGE, CONTENT) must not execute before table-level permission checks, preventing data exfiltration via THROW or side effects"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
SELECT * FROM accounts WHERE THROW 'leaked: ' + secret;

-- Same for UPDATE WHERE
UPDATE accounts SET x = 1 WHERE THROW 'leaked: ' + secret;

-- Same for DELETE WHERE
DELETE accounts WHERE THROW 'leaked: ' + secret;

-- Same for UPSERT WHERE
UPSERT accounts SET x = 1 WHERE THROW 'leaked: ' + secret;

-- SET expressions must also not execute before table-level permission checks.
UPDATE accounts SET x = THROW 'leaked: ' + secret;

-- MERGE expressions evaluated against the live record
UPDATE accounts MERGE { x: THROW 'leaked: ' + secret };

-- CONTENT expressions evaluated against the live record
UPDATE accounts CONTENT { x: THROW 'leaked: ' + secret };

-- UPSERT SET expressions must not execute before table-level permission checks
-- on the update path. The WHERE clause makes UPSERT non-deferable, so the
-- iterator loads existing records via Iterable::RecordId and routes directly
-- to upsert_update (exercising the check_pre_update ordering fix).
UPSERT accounts SET x = THROW 'leaked: ' + secret WHERE true;

-- INSERT ON DUPLICATE KEY UPDATE exercises the insert_update path.
-- Uses a non-throwing expression because `stm.data()` for INSERT returns
-- the update clause, which insert_create evaluates before the RecordExists
-- fallback. The insert_update path must still block via update permission.
INSERT INTO accounts { id: accounts:1, x: 0 } ON DUPLICATE KEY UPDATE x = secret;

-- RELATE on an existing edge exercises the relate_update path
RELATE accounts:1 -> friends:1 -> accounts:2 SET x = THROW 'leaked: ' + secret;
