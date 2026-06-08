/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter wire-fast path for tag-only literals: WHERE field = true / = false / = NULL / = NONE bytewise-compares the runtime value's wire tag against the pre-encoded literal, without decoding the leaf Value. EXPLAIN must still report pre_decode_filter: yes."

# CREATE ONLY returns the single value (no array wrap); `flag = NONE`
# never lands in the stored object so it's omitted from the result.
[[test.results]]
value = "{ active: true, deleted: NULL, id: ptag:1 }"

[[test.results]]
value = "{ active: false, deleted: 1, flag: 'set', id: ptag:2 }"

[[test.results]]
value = "{ active: true, deleted: NULL, id: ptag:3 }"

# WHERE active = true -> ptag:1, ptag:3
[[test.results]]
value = "[{ id: ptag:1 }, { id: ptag:3 }]"

# WHERE active = false -> ptag:2
[[test.results]]
value = "[{ id: ptag:2 }]"

# WHERE deleted = NULL -> ptag:1, ptag:3
[[test.results]]
value = "[{ id: ptag:1 }, { id: ptag:3 }]"

# WHERE flag = NONE -> ptag:1, ptag:3 (and any row with `flag` absent or explicitly NONE)
[[test.results]]
value = "[{ id: ptag:1 }, { id: ptag:3 }]"

# WHERE active != true -> ptag:2
[[test.results]]
value = "[{ id: ptag:2 }]"

# EXPLAIN for each shape: predicate is preserved in the EXPLAIN output and
# pre_decode_filter: yes proves the wire-fast tag-peek path was selected.
[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ptag, direction: Forward, predicate: active = true, pre_decode_filter: yes]
"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ptag, direction: Forward, predicate: deleted = NULL, pre_decode_filter: yes]
"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: ptag, direction: Forward, predicate: flag = NONE, pre_decode_filter: yes]
"'''

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
