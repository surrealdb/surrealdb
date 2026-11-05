/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
A condition through a record link (`link.v = 5`) is answered from the linked
table's indexes, and a condition on the row's own fields (`a = 1`) from the
row's. The two tables' index lists are separate, so an index's position in one
never names an index in the other. Here `a_set` flattens a set field and is
not sought for containment, `b_ab` follows it on `x`, and `v_wv` is the first
index on `t`; `link.v = 5` still narrows only `v_wv`, so SELECT, UPDATE and
DELETE all reach `x:1`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: 5, w: 9 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, id: x:1, link: t:1, s: {1,} }]"

[[test.results]]
value = "[x:1]"

[[test.results]]
value = "[x:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX v_wv ON t FIELDS w, v;
CREATE t:1 SET w = 9, v = 5;
DEFINE TABLE x SCHEMALESS;
DEFINE FIELD s ON x TYPE set<int>;
DEFINE FIELD link ON x TYPE record<t>;
DEFINE INDEX a_set ON x FIELDS s.*;
DEFINE INDEX b_ab ON x FIELDS a, b;
DEFINE INDEX c_link ON x FIELDS link;
CREATE x:1 SET a = 1, b = 2, link = t:1, s = <set>[1];
SELECT VALUE id FROM x WHERE a = 1 AND link.v = 5;
UPDATE x SET hit = true WHERE a = 1 AND link.v = 5 RETURN VALUE id;
DELETE x WHERE a = 1 AND link.v = 5 RETURN NONE;
-- The DELETE above removed the one matching record.
SELECT VALUE id FROM x;
