/**
[env]
imports = ["reproductions/where_permissions_data_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "accounts:1" }

[test]
reason = """
User-supplied expressions (WHERE, SET, MERGE, CONTENT) must not execute before
table-level permission checks, preventing data exfiltration via THROW or side
effects.

The guaranteed record an `UPSERT … WHERE` writes when it matched nothing is
written only on a table the caller may create records on; on any other its data
clause is never evaluated. Where it may, the record is new, so the clause reads
an empty document and the failure it reports names no stored value, and the
statement fails with it, since a guaranteed record that could not be written is
a failed write.
"""

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
error = "Cannot perform addition with 'string' and 'none'"

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

-- RELATE on an existing edge still routes through `relate_update` (only
-- INSERT RELATION with an explicit id became create-only), so the table-level
-- update permission check must run before the SET THROW expression.
RELATE accounts:1 -> friends:1 -> accounts:2 SET x = THROW 'leaked: ' + secret;

-- UPDATE on the same existing relation record covers the generic update
-- path's permission ordering for completeness.
UPDATE friends:1 SET x = THROW 'leaked: ' + secret;
