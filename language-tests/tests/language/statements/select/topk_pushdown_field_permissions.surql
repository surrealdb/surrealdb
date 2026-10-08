/**
[env]
namespace = true
database = true

[test]
reason = "TopK threshold pushdown with a field-level SELECT permission on the ORDER BY field, read as the owner: the owner is not permission-checked, so the bound plan carries no guard, the scan keeps `topk_pushdown: yes`, and the order reflects the stored values. The record-user side is pinned in topk_pushdown_field_permissions_record_user.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, pub: 1, secret: 3 }]"

[[test.results]]
value = "[{ id: p:2, pub: 2, secret: 1 }]"

[[test.results]]
value = "[{ id: p:3, pub: 3, secret: 2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: secret DESC, keep: 2]
            TableScan [ctx: Db] [table: p, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[p:1, p:3]"

[[test.results]]
value = "[p:2, p:3]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: pub DESC, keep: 2]
            TableScan [ctx: Db] [table: p, direction: Forward, topk_pushdown: yes]
''''
*/

DEFINE TABLE p SCHEMALESS;
DEFINE FIELD secret ON p TYPE int PERMISSIONS FOR select WHERE false;
CREATE p:1 SET secret = 3, pub = 1;
CREATE p:2 SET secret = 1, pub = 2;
CREATE p:3 SET secret = 2, pub = 3;

-- The owner is not permission-checked, so the guard on `secret` enters no
-- plan and the ORDER BY field stays eligible for the threshold probe.
EXPLAIN SELECT * FROM p ORDER BY secret DESC LIMIT 2;

-- The order reflects the stored values.
SELECT VALUE id FROM p ORDER BY secret DESC LIMIT 2;
SELECT VALUE id FROM p ORDER BY secret ASC LIMIT 2;

-- An unrestricted field on the same table stays fully eligible.
EXPLAIN SELECT * FROM p ORDER BY pub DESC LIMIT 2;
