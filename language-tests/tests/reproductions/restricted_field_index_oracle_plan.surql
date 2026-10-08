/**
[env]
imports = ["reproductions/restricted_field_index_oracle_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }


[test]
reason = "Plan-level companion to `restricted_field_index_oracle`. Result-only assertions cannot separate `the restricted index was withheld` from `every index was withheld`, because a table scan returns the same rows. These pin the access path itself: no index answers a predicate over the restricted `secret` column (neither the single-column `idx_secret` nor the compound `idx_cat_secret`), while `idx_category` — whose only column the actor may select — is still chosen, including as the driving index for the mixed predicate. The caller's field guard is the `secret` column of the `ProjectLayer` beneath the user's `Filter`; the predicate reads `secret` as that layer presents it, which no index may answer, so that conjunct is judged above the layer; the `category` conjunct reads no guarded column, moves beneath the layer, and is answered by the index in full."

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret = 'S3CRET_TOKEN_VALUE']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: docs, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret = 'S3CRET_TOKEN_VALUE']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            IndexScan [ctx: Db] [table: docs, index: idx_category, access: = 'a', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: docs, index: idx_category, access: = 'a', direction: Forward]
"'''

*/

EXPLAIN SELECT id FROM docs WHERE secret = 'S3CRET_TOKEN_VALUE';
EXPLAIN SELECT id FROM docs WHERE secret = 'S3CRET_TOKEN_VALUE' AND category = 'a';
EXPLAIN SELECT id FROM docs WHERE category = 'a';
