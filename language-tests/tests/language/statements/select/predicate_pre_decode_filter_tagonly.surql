/**
[env]
namespace = true
database = true

[test]
reason = "Pre-decode filter wire-fast path for tag-only literals: WHERE field = true / = false / = NULL / = NONE bytewise-compares the runtime value's wire tag against the pre-encoded literal, without decoding the leaf Value. EXPLAIN shows the predicate as a filter over the table scan."

[[test.results]]
value = "{ active: true, deleted: NULL, id: ptag:1 }"

[[test.results]]
value = "{ active: false, deleted: 1, flag: 'set', id: ptag:2 }"

[[test.results]]
value = "{ active: true, deleted: NULL, id: ptag:3 }"

[[test.results]]
value = "[{ id: ptag:1 }, { id: ptag:3 }]"

[[test.results]]
value = "[{ id: ptag:2 }]"

[[test.results]]
value = "[{ id: ptag:1 }, { id: ptag:3 }]"

[[test.results]]
value = "[{ id: ptag:1 }, { id: ptag:3 }]"

[[test.results]]
value = "[{ id: ptag:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ptag, direction: Forward, predicate: active = true, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ptag, direction: Forward, predicate: deleted = NULL, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ptag, direction: Forward, predicate: flag = NONE, pre_decode_filter: yes]
''''

*/

CREATE ONLY ptag:1 SET active = true, deleted = NULL, flag = NONE;
CREATE ONLY ptag:2 SET active = false, deleted = 1, flag = 'set';
CREATE ONLY ptag:3 SET active = true, deleted = NULL, flag = NONE;

SELECT id FROM ptag WHERE active = true;
SELECT id FROM ptag WHERE active = false;
SELECT id FROM ptag WHERE deleted = NULL;
SELECT id FROM ptag WHERE flag = NONE;
SELECT id FROM ptag WHERE active != true;

EXPLAIN SELECT id FROM ptag WITH NOINDEX WHERE active = true;
EXPLAIN SELECT id FROM ptag WITH NOINDEX WHERE deleted = NULL;
EXPLAIN SELECT id FROM ptag WITH NOINDEX WHERE flag = NONE;
