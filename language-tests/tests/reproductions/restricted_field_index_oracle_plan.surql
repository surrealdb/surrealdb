/**
[env]
imports = ["reproductions/restricted_field_index_oracle_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

planner-strategy = ["all-ro"]

[test]
reason = "Plan-level companion to `restricted_field_index_oracle`. Result-only assertions cannot separate `the restricted index was withheld` from `every index was withheld`, because a table scan returns the same rows. These pin the access path itself: no index answers a predicate over the restricted `secret` column (neither the single-column `idx_secret` nor the compound `idx_cat_secret`), while `idx_category` — whose only column the actor may select — is still chosen, including as the driving index for the mixed predicate."

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'deferred (runtime field state)', predicate: "secret = 'S3CRET_TOKEN_VALUE'", table: 'docs' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "secret = 'S3CRET_TOKEN_VALUE'" }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_category' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', expressions: [{ role: 'predicate', sql: "secret = 'S3CRET_TOKEN_VALUE'" }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_category' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

*/

SELECT id FROM docs WHERE secret = 'S3CRET_TOKEN_VALUE' EXPLAIN;
SELECT id FROM docs WHERE secret = 'S3CRET_TOKEN_VALUE' AND category = 'a' EXPLAIN;
SELECT id FROM docs WHERE category = 'a' EXPLAIN;
