/**
[env]
namespace = true
database = true
auth = { level = "owner" }

planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Index COLUMN SHAPE against a fixed operator set (=, a range, CONTAINS), so the
shape of the indexed column is the only variable. Each case asserts the plan,
the rows the plan produced, and the same query under WITH NOINDEX, which forces
a table scan and is therefore the correctness oracle: a plan that names an
IndexScan while disagreeing with the oracle is the failure mode this file
exists to catch. Wherever an index over a column that can fan out (an array
value, or a column carrying `*`) is actually seeked, the count()/GROUP ALL
pair is asserted too, because the count goes through IndexCountScan, a path
that can disagree with both.

A column whose idiom carries `*` stores one index entry per distinct element of
that column's value, so a key range which does not pin that column reaches one
record through several entries. Such a scan is wrapped in `Distinct [ctx: Db]`,
directly above the IndexScan and below any residual Filter, and that node is
what makes each matching record reach the rest of the plan exactly once; under
EXPLAIN ANALYZE the scan still reports the entry count in its own {rows: N}
while the Distinct above it reports the record count. A scan that pins its
fanning column does not fan out and carries no Distinct. Shape E asserts the
pinned side; compound_prefix.surql asserts the contrast, because the node's
presence is the only thing in the plan text separating an exact seek from a
seek-then-collapse.

Both streaming strategies are pinned because a prefix EXPLAIN errors under
compute-only; all-ro and best-effort-ro plan identically by contract, so
pinning both records that and costs nothing.

Two shapes record WRONG answers. They are pinned deliberately, each
labelled at its case:

1. The record dedupe keys off the `*` in a column idiom, so a plain (non-`*`)
   column whose VALUE is an array is not covered by it: such a column fans out
   on write all the same, so a range predicate emits the record once per
   qualifying element, and drops a record whose whole-array comparison is true
   while all of its elements fall outside the range. Correct: the oracle's
   distinct row set.
2. IndexCountScan on such a plain column counts index entries, not records, and
   does not apply the per-record recheck that the row path applies. Correct:
   the oracle's count.

Whole-array equality against a plain array column belongs to the fan-out key
model rather than to the column-shape axis, and is pinned once, in
array_fanout_oracle.surql.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sh_flat:1, score: 10 }]"

[[test.results]]
value = "[{ id: sh_flat:2, score: 20 }]"

[[test.results]]
value = "[{ id: sh_flat:3, score: 30 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 20, direction: Forward, predicate: score = 20]
''''

[[test.results]]
value = "[{ id: sh_flat:2 }]"

[[test.results]]
value = "[{ id: sh_flat:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 20, direction: Forward, predicate: 20 = score]
''''

[[test.results]]
value = "[{ id: sh_flat:2 }]"

[[test.results]]
value = "[{ id: sh_flat:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >15, direction: Forward, predicate: score > 15]
''''

[[test.results]]
value = "[{ id: sh_flat:2 }, { id: sh_flat:3 }]"

[[test.results]]
value = "[{ id: sh_flat:2 }, { id: sh_flat:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_flat, direction: Forward, predicate: score != 20, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: sh_flat:1 }, { id: sh_flat:3 }]"

[[test.results]]
value = "[{ id: sh_flat:1 }, { id: sh_flat:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: 10, other: 'x' }, id: sh_nested:1 }]"

[[test.results]]
value = "[{ a: { b: 20, other: 'y' }, id: sh_nested:2 }]"

[[test.results]]
value = "[{ a: { b: 30, other: 'z' }, id: sh_nested:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 20, direction: Forward, predicate: a.b = 20]
''''

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 20, direction: Forward, predicate: 20 = a.b]
''''

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >=20, direction: Forward, predicate: a.b >= 20]
''''

[[test.results]]
value = "[{ id: sh_nested:2 }, { id: sh_nested:3 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }, { id: sh_nested:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >15, direction: Forward, predicate: 15 < a.b]
''''

[[test.results]]
value = "[{ id: sh_nested:2 }, { id: sh_nested:3 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }, { id: sh_nested:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >10 <30, direction: Forward, predicate: a.b > 10 AND a.b < 30]
''''

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_nested, direction: Forward, predicate: a.other = 'y', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_nested, direction: Forward, predicate: a = { b: 20, other: 'y' }, pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = "[{ id: sh_nested:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: { c: 10 } }, id: sh_deep:1 }]"

[[test.results]]
value = "[{ a: { b: { c: 20 } }, id: sh_deep:2 }]"

[[test.results]]
value = "[{ a: { b: { c: 30 } }, id: sh_deep:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 20, direction: Forward, predicate: a.b.c = 20]
''''

[[test.results]]
value = "[{ id: sh_deep:2 }]"

[[test.results]]
value = "[{ id: sh_deep:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >=20, direction: Forward, predicate: a.b.c >= 20]
''''

[[test.results]]
value = "[{ id: sh_deep:2 }, { id: sh_deep:3 }]"

[[test.results]]
value = "[{ id: sh_deep:2 }, { id: sh_deep:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_deep, direction: Forward, predicate: a.b = { c: 20 }, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: sh_deep:2 }]"

[[test.results]]
value = "[{ id: sh_deep:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_deep, direction: Forward, predicate: a.b.c != 20, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: sh_deep:1 }, { id: sh_deep:3 }]"

[[test.results]]
value = "[{ id: sh_deep:1 }, { id: sh_deep:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: 1, c: 'x' }, id: sh_compound_nested:1 }]"

[[test.results]]
value = "[{ a: { b: 1, c: 'y' }, id: sh_compound_nested:2 }]"

[[test.results]]
value = "[{ a: { b: 2, c: 'x' }, id: sh_compound_nested:3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: [1, 'y'], direction: Forward, predicate: a.b = 1 AND a.c = 'y']
"'''

[[test.results]]
value = "[{ id: sh_compound_nested:2 }]"

[[test.results]]
value = "[{ id: sh_compound_nested:2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: [1], direction: Forward, predicate: a.b = 1]
''''

[[test.results]]
value = "[{ id: sh_compound_nested:1 }, { id: sh_compound_nested:2 }]"

[[test.results]]
value = "[{ id: sh_compound_nested:1 }, { id: sh_compound_nested:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_compound_nested, direction: Forward, predicate: a.c = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: sh_compound_nested:1 }, { id: sh_compound_nested:3 }]"

[[test.results]]
value = "[{ id: sh_compound_nested:1 }, { id: sh_compound_nested:3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: [1] MoreThan 'x', direction: Forward, predicate: a.b = 1 AND a.c > 'x']
"'''

[[test.results]]
value = "[{ id: sh_compound_nested:2 }]"

[[test.results]]
value = "[{ id: sh_compound_nested:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sh_flatten:1, tags: ['red', 'blue'] }]"

[[test.results]]
value = "[{ id: sh_flatten:2, tags: ['green'] }]"

[[test.results]]
value = "[{ id: sh_flatten:3, tags: ['blue', 'red', 'blue'] }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 'blue', direction: Forward, predicate: tags CONTAINS 'blue']
"'''

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:3 }]"

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 2}
    IndexScan [ctx: Db] [index: idx, access: = 'blue', direction: Forward, predicate: tags CONTAINS 'blue'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_flatten, condition: tags CONTAINS 'blue']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_flatten, direction: Forward, predicate: tags = 'blue', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_flatten, direction: Forward, predicate: tags.* > 'c', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:2 }, { id: sh_flatten:3 }]"

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:2 }, { id: sh_flatten:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_flatten, direction: Forward]
''''

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:2 }, { id: sh_flatten:3 }]"

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:2 }, { id: sh_flatten:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    TableScan [ctx: Db] [table: sh_flatten, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: sh_flatten]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_flatten, direction: Forward]
''''

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:2 }, { id: sh_flatten:3 }]"

[[test.results]]
value = "[{ id: sh_flatten:1 }, { id: sh_flatten:2 }, { id: sh_flatten:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sh_bracket:1, tags: ['red', 'blue'] }]"

[[test.results]]
value = "[{ id: sh_bracket:2, tags: ['green'] }]"

[[test.results]]
value = "[{ id: sh_bracket:3, tags: ['blue', 'red', 'blue'] }]"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx: 'DEFINE INDEX idx ON sh_bracket FIELDS tags.*' }, lives: {  }, tables: {  } }"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 'blue', direction: Forward, predicate: tags CONTAINS 'blue']
"'''

[[test.results]]
value = "[{ id: sh_bracket:1 }, { id: sh_bracket:3 }]"

[[test.results]]
value = "[{ id: sh_bracket:1 }, { id: sh_bracket:3 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_bracket, condition: tags CONTAINS 'blue']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emails: [{ norm: 'a@x', raw: 'A@x' }, { norm: 'b@x', raw: 'B@x' }], id: sh_objarr:1 }]"

[[test.results]]
value = "[{ emails: [{ norm: 'c@x', raw: 'C@x' }], id: sh_objarr:2 }]"

[[test.results]]
value = "[{ emails: [{ norm: 'b@x', raw: 'B@x' }, { norm: 'd@x', raw: 'D@x' }], id: sh_objarr:3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 'b@x', direction: Forward, predicate: emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = "[{ id: sh_objarr:1 }, { id: sh_objarr:3 }]"

[[test.results]]
value = "[{ id: sh_objarr:1 }, { id: sh_objarr:3 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_objarr, condition: emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_objarr, direction: Forward, predicate: emails.*.norm = 'b@x', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_objarr, direction: Forward, predicate: emails.*.raw CONTAINS 'B@x', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: sh_objarr:1 }, { id: sh_objarr:3 }]"

[[test.results]]
value = "[{ id: sh_objarr:1 }, { id: sh_objarr:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [{ b: ['p', 'q'] }, { b: ['r'] }], id: sh_twolevel:1 }]"

[[test.results]]
value = "[{ a: [{ b: ['s'] }], id: sh_twolevel:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 'q', direction: Forward, predicate: a.*.b.* CONTAINS 'q']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_twolevel, condition: a.*.b.* CONTAINS 'q']
"'''

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = ['p', 'q'], direction: Forward, predicate: a.*.b.* CONTAINS ['p', 'q']]
"'''

[[test.results]]
value = "[{ id: sh_twolevel:1 }]"

[[test.results]]
value = "[{ id: sh_twolevel:1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_twolevel, condition: a.*.b.* CONTAINS ['p', 'q']]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = ['p', 'q'], direction: Forward, predicate: a.*.b CONTAINS ['p', 'q']]
"'''

[[test.results]]
value = "[{ id: sh_twolevel:1 }]"

[[test.results]]
value = "[{ id: sh_twolevel:1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_twolevel, condition: a.*.b CONTAINS ['p', 'q']]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sh_idelem:[1, 'x'] }]"

[[test.results]]
value = "[{ id: sh_idelem:[2, 'y'] }]"

[[test.results]]
value = "[{ id: sh_idelem:[3, 'x'] }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 'x', direction: Forward, predicate: id[1] = 'x']
"'''

[[test.results]]
value = "[{ id: sh_idelem:[1, 'x'] }, { id: sh_idelem:[3, 'x'] }]"

[[test.results]]
value = "[{ id: sh_idelem:[1, 'x'] }, { id: sh_idelem:[3, 'x'] }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >'x', direction: Forward, predicate: id[1] > 'x']
"'''

[[test.results]]
value = "[{ id: sh_idelem:[2, 'y'] }]"

[[test.results]]
value = "[{ id: sh_idelem:[2, 'y'] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_idelem, direction: Forward, predicate: id[0] = 1, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[{ id: sh_idelem:[1, 'x'] }]"

[[test.results]]
value = "[{ id: sh_idelem:[1, 'x'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sh_arrayvalue:1, tags: ['red', 'tan'] }]"

[[test.results]]
value = "[{ id: sh_arrayvalue:2, tags: 'red' }]"

[[test.results]]
value = "[{ id: sh_arrayvalue:3, tags: ['ant'] }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: = 'red', direction: Forward, predicate: tags = 'red']
"'''

[[test.results]]
value = "[{ id: sh_arrayvalue:2 }]"

[[test.results]]
value = "[{ id: sh_arrayvalue:2 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_arrayvalue, condition: tags = 'red']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: >'p', direction: Forward, predicate: tags > 'p']
"'''

[[test.results]]
value = "[{ id: sh_arrayvalue:1 }, { id: sh_arrayvalue:2 }, { id: sh_arrayvalue:1 }]"

[[test.results]]
value = "[{ id: sh_arrayvalue:1 }, { id: sh_arrayvalue:2 }, { id: sh_arrayvalue:3 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: sh_arrayvalue, condition: tags > 'p']
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_arrayvalue, direction: Forward, predicate: tags CONTAINS 'tan', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: sh_arrayvalue:1 }]"

[[test.results]]
value = "[{ id: sh_arrayvalue:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: sh_arrayvalue, direction: Forward]
''''

[[test.results]]
value = "[{ id: sh_arrayvalue:1 }, { id: sh_arrayvalue:2 }, { id: sh_arrayvalue:3 }]"

[[test.results]]
value = "[{ id: sh_arrayvalue:1 }, { id: sh_arrayvalue:2 }, { id: sh_arrayvalue:3 }]"
*/

-- ===========================================================================
-- Shape A: flat scalar column. The baseline every other shape is read against
-- - a narrow `access:` proves the seek, and `predicate:` records that the scan
-- still rechecks the clause per fetched row.
-- ===========================================================================

DEFINE TABLE sh_flat SCHEMALESS;
DEFINE INDEX idx ON sh_flat COLUMNS score;
CREATE sh_flat:1 SET score = 10;
CREATE sh_flat:2 SET score = 20;
CREATE sh_flat:3 SET score = 30;

-- A1: equality.
EXPLAIN SELECT id FROM sh_flat WHERE score = 20;
SELECT id FROM sh_flat WHERE score = 20;
SELECT id FROM sh_flat WITH NOINDEX WHERE score = 20;

-- A2: the literal on the left. Operand order must not change the seek.
EXPLAIN SELECT id FROM sh_flat WHERE 20 = score;
SELECT id FROM sh_flat WHERE 20 = score;
SELECT id FROM sh_flat WITH NOINDEX WHERE 20 = score;

-- A3: range.
EXPLAIN SELECT id FROM sh_flat WHERE score > 15;
SELECT id FROM sh_flat WHERE score > 15;
SELECT id FROM sh_flat WITH NOINDEX WHERE score > 15;

-- A4: inequality. Not a range over a single key, so no index applies and the
-- predicate is pushed into the table scan instead.
EXPLAIN SELECT id FROM sh_flat WHERE score != 20;
SELECT id FROM sh_flat WHERE score != 20;
SELECT id FROM sh_flat WITH NOINDEX WHERE score != 20;

-- ===========================================================================
-- Shape B: nested column `a.b`. The index seeks on a path the record does not
-- carry at top level, so the seek key must be built from the traversed value.
-- ===========================================================================

DEFINE TABLE sh_nested SCHEMALESS;
DEFINE INDEX idx ON sh_nested COLUMNS a.b;
CREATE sh_nested:1 SET a = { b: 10, other: 'x' };
CREATE sh_nested:2 SET a = { b: 20, other: 'y' };
CREATE sh_nested:3 SET a = { b: 30, other: 'z' };

-- B1: equality on a nested column.
EXPLAIN SELECT id FROM sh_nested WHERE a.b = 20;
SELECT id FROM sh_nested WHERE a.b = 20;
SELECT id FROM sh_nested WITH NOINDEX WHERE a.b = 20;

-- B2: the literal on the left of a nested column.
EXPLAIN SELECT id FROM sh_nested WHERE 20 = a.b;
SELECT id FROM sh_nested WHERE 20 = a.b;
SELECT id FROM sh_nested WITH NOINDEX WHERE 20 = a.b;

-- B3: range on a nested column.
EXPLAIN SELECT id FROM sh_nested WHERE a.b >= 20;
SELECT id FROM sh_nested WHERE a.b >= 20;
SELECT id FROM sh_nested WITH NOINDEX WHERE a.b >= 20;

-- B4: range with the literal on the left. `15 < a.b` must seek the same range
-- as `a.b > 15`, with the bound flipped rather than dropped.
EXPLAIN SELECT id FROM sh_nested WHERE 15 < a.b;
SELECT id FROM sh_nested WHERE 15 < a.b;
SELECT id FROM sh_nested WITH NOINDEX WHERE 15 < a.b;

-- B5: a bounded range over a nested column, from both sides.
EXPLAIN SELECT id FROM sh_nested WHERE a.b > 10 AND a.b < 30;
SELECT id FROM sh_nested WHERE a.b > 10 AND a.b < 30;
SELECT id FROM sh_nested WITH NOINDEX WHERE a.b > 10 AND a.b < 30;

-- B6: a sibling of the indexed column is not the indexed column.
EXPLAIN SELECT id FROM sh_nested WHERE a.other = 'y';
SELECT id FROM sh_nested WHERE a.other = 'y';
SELECT id FROM sh_nested WITH NOINDEX WHERE a.other = 'y';

-- B7: the parent object is not the indexed column either. An index on `a.b`
-- must not be seeked for a predicate on `a`.
EXPLAIN SELECT id FROM sh_nested WHERE a = { b: 20, other: 'y' };
SELECT id FROM sh_nested WHERE a = { b: 20, other: 'y' };
SELECT id FROM sh_nested WITH NOINDEX WHERE a = { b: 20, other: 'y' };

-- ===========================================================================
-- Shape C: deeply nested column `a.b.c`. Same contract as B at one more level
-- of traversal.
-- ===========================================================================

DEFINE TABLE sh_deep SCHEMALESS;
DEFINE INDEX idx ON sh_deep COLUMNS a.b.c;
CREATE sh_deep:1 SET a = { b: { c: 10 } };
CREATE sh_deep:2 SET a = { b: { c: 20 } };
CREATE sh_deep:3 SET a = { b: { c: 30 } };

-- C1: equality three levels down.
EXPLAIN SELECT id FROM sh_deep WHERE a.b.c = 20;
SELECT id FROM sh_deep WHERE a.b.c = 20;
SELECT id FROM sh_deep WITH NOINDEX WHERE a.b.c = 20;

-- C2: range three levels down.
EXPLAIN SELECT id FROM sh_deep WHERE a.b.c >= 20;
SELECT id FROM sh_deep WHERE a.b.c >= 20;
SELECT id FROM sh_deep WITH NOINDEX WHERE a.b.c >= 20;

-- C3: an intermediate prefix of the indexed path is not the indexed column.
EXPLAIN SELECT id FROM sh_deep WHERE a.b = { c: 20 };
SELECT id FROM sh_deep WHERE a.b = { c: 20 };
SELECT id FROM sh_deep WITH NOINDEX WHERE a.b = { c: 20 };

-- C4: inequality three levels down falls back to a table scan, as at shape A.
EXPLAIN SELECT id FROM sh_deep WHERE a.b.c != 20;
SELECT id FROM sh_deep WHERE a.b.c != 20;
SELECT id FROM sh_deep WITH NOINDEX WHERE a.b.c != 20;

-- ===========================================================================
-- Shape D: compound index whose columns are both nested. The prefix rule must
-- hold over traversed paths exactly as it does over flat ones.
-- ===========================================================================

DEFINE TABLE sh_compound_nested SCHEMALESS;
DEFINE INDEX idx ON sh_compound_nested COLUMNS a.b, a.c;
CREATE sh_compound_nested:1 SET a = { b: 1, c: 'x' };
CREATE sh_compound_nested:2 SET a = { b: 1, c: 'y' };
CREATE sh_compound_nested:3 SET a = { b: 2, c: 'x' };

-- D1: both columns pinned - a full compound key seek.
EXPLAIN SELECT id FROM sh_compound_nested WHERE a.b = 1 AND a.c = 'y';
SELECT id FROM sh_compound_nested WHERE a.b = 1 AND a.c = 'y';
SELECT id FROM sh_compound_nested WITH NOINDEX WHERE a.b = 1 AND a.c = 'y';

-- D2: leading column only - a prefix seek.
EXPLAIN SELECT id FROM sh_compound_nested WHERE a.b = 1;
SELECT id FROM sh_compound_nested WHERE a.b = 1;
SELECT id FROM sh_compound_nested WITH NOINDEX WHERE a.b = 1;

-- D3: trailing column only. Not a prefix, so the index cannot be seeked.
EXPLAIN SELECT id FROM sh_compound_nested WHERE a.c = 'x';
SELECT id FROM sh_compound_nested WHERE a.c = 'x';
SELECT id FROM sh_compound_nested WITH NOINDEX WHERE a.c = 'x';

-- D4: leading column pinned, trailing column ranged.
EXPLAIN SELECT id FROM sh_compound_nested WHERE a.b = 1 AND a.c > 'x';
SELECT id FROM sh_compound_nested WHERE a.b = 1 AND a.c > 'x';
SELECT id FROM sh_compound_nested WITH NOINDEX WHERE a.b = 1 AND a.c > 'x';

-- ===========================================================================
-- Shape E: array-flatten column `tags.*`. The write path stores one entry per
-- distinct element of the column's value, so one record is reachable through
-- several entries; the count is therefore asserted alongside the rows wherever
-- the index is seeked, and sh_flatten:3 carries a repeated element so an entry
-- layout that kept within-record duplicates could not hide.
--
-- Whether a scan of this index fans out is what the `Distinct` node records. A
-- key range that pins the flatten column to a single value reaches each record
-- through at most one entry and carries no `Distinct` (E1). A range that
-- leaves it unpinned is wrapped in `Distinct [ctx: Db]`, directly above the
-- IndexScan and below any residual Filter, and that is what makes each
-- matching record reach the rest of the plan exactly once; this shape reaches
-- no such range, and compound_prefix.surql asserts that side of the contrast.
-- ===========================================================================

DEFINE TABLE sh_flatten SCHEMALESS;
DEFINE INDEX idx ON sh_flatten COLUMNS tags.*;
CREATE sh_flatten:1 SET tags = ['red', 'blue'];
CREATE sh_flatten:2 SET tags = ['green'];
CREATE sh_flatten:3 SET tags = ['blue', 'red', 'blue'];

-- E1: CONTAINS is the operator the flatten column exists for. A single
-- containment value pins the flatten column, so the seek is exact - a narrow
-- `access:`, a `predicate:` recheck on each fetched row, and no `Distinct`
-- above the scan. sh_flatten:3 is reached through one entry, because its
-- repeated element is stored once.
EXPLAIN SELECT id FROM sh_flatten WHERE tags CONTAINS 'blue';
SELECT id FROM sh_flatten WHERE tags CONTAINS 'blue';
SELECT id FROM sh_flatten WITH NOINDEX WHERE tags CONTAINS 'blue';

-- E2: E1 under ANALYZE. The scan's own {rows: N} counts the index entries it
-- read; with the flatten column pinned that is already the record count, so
-- nothing above the scan has to collapse anything. E7 is the same measurement
-- over a range that does fan out.
EXPLAIN ANALYZE SELECT id FROM sh_flatten WHERE tags CONTAINS 'blue';

-- E3: the count of E1, which goes through IndexCountScan rather than the row
-- path, so it can disagree with both the indexed rows and the oracle. The
-- index-only count is offered only to a scan that does not fan out, which the
-- pinned flatten column of E1 satisfies.
EXPLAIN SELECT count() FROM sh_flatten WHERE tags CONTAINS 'blue' GROUP ALL;
SELECT count() FROM sh_flatten WHERE tags CONTAINS 'blue' GROUP ALL;
SELECT count() FROM sh_flatten WITH NOINDEX WHERE tags CONTAINS 'blue' GROUP ALL;

-- E4: scalar equality against the whole array column. A flatten column is
-- offered only to containment predicates, so equality is not seeked at all and
-- the clause is pushed into a table scan. The oracle agrees at [], because an
-- array does not equal a scalar.
EXPLAIN SELECT id FROM sh_flatten WHERE tags = 'blue';
SELECT id FROM sh_flatten WHERE tags = 'blue';
SELECT id FROM sh_flatten WITH NOINDEX WHERE tags = 'blue';

-- E5: a range over the flatten idiom itself is not indexable.
EXPLAIN SELECT id FROM sh_flatten WHERE tags.* > 'c';
SELECT id FROM sh_flatten WHERE tags.* > 'c';
SELECT id FROM sh_flatten WITH NOINDEX WHERE tags.* > 'c';

-- E6: an unfiltered `ORDER BY id` is not served by the `tags.*` index. A
-- full-range read of it walks element order - a record sits at one key
-- position per element - so the scan claims no ordering past the flatten
-- column, and none by record id either. With nothing to gain from the index,
-- the plan is a table scan, which delivers id order: the rows and their order
-- are the oracle's.
EXPLAIN SELECT id FROM sh_flatten ORDER BY id;
SELECT id FROM sh_flatten ORDER BY id;
SELECT id FROM sh_flatten WITH NOINDEX ORDER BY id;

-- E7: E6 under ANALYZE. The table scan reads each record once, so its row
-- count is the record count E8 reaches without the index.
EXPLAIN ANALYZE SELECT id FROM sh_flatten ORDER BY id;

-- E8: the count over the same table, which is planned as a CountScan rather
-- than through the fanning index and so counts records. It is the third leg of
-- E6: the rows, the oracle, and a record count reached without the index at
-- all.
EXPLAIN SELECT count() FROM sh_flatten GROUP ALL;
SELECT count() FROM sh_flatten GROUP ALL;
SELECT count() FROM sh_flatten WITH NOINDEX GROUP ALL;

-- E9: the same shape with no ORDER BY is planned as a table scan, as E6 is.
EXPLAIN SELECT id FROM sh_flatten;
SELECT id FROM sh_flatten;
SELECT id FROM sh_flatten WITH NOINDEX;

-- ===========================================================================
-- Shape F: the bracket spelling `tags[*]`. A spelling of shape E, not a
-- different shape - INFO FOR TABLE shows the stored definition normalised, and
-- the plan must be the one shape E produces.
-- ===========================================================================

DEFINE TABLE sh_bracket SCHEMALESS;
DEFINE INDEX idx ON sh_bracket COLUMNS tags[*];
CREATE sh_bracket:1 SET tags = ['red', 'blue'];
CREATE sh_bracket:2 SET tags = ['green'];
CREATE sh_bracket:3 SET tags = ['blue', 'red', 'blue'];

-- F1: the definition as stored.
INFO FOR TABLE sh_bracket;

-- F2: same plan and same rows as E1, `Distinct`-free for the same reason.
EXPLAIN SELECT id FROM sh_bracket WHERE tags CONTAINS 'blue';
SELECT id FROM sh_bracket WHERE tags CONTAINS 'blue';
SELECT id FROM sh_bracket WITH NOINDEX WHERE tags CONTAINS 'blue';

-- F3: same count plan and same count as E3.
EXPLAIN SELECT count() FROM sh_bracket WHERE tags CONTAINS 'blue' GROUP ALL;
SELECT count() FROM sh_bracket WHERE tags CONTAINS 'blue' GROUP ALL;
SELECT count() FROM sh_bracket WITH NOINDEX WHERE tags CONTAINS 'blue' GROUP ALL;

-- ===========================================================================
-- Shape G: array-of-objects element `emails.*.norm`. Flatten then traverse -
-- one entry per element of the array, keyed on a field inside the element.
-- ===========================================================================

DEFINE TABLE sh_objarr SCHEMALESS;
DEFINE INDEX idx ON sh_objarr COLUMNS emails.*.norm;
CREATE sh_objarr:1 SET emails = [{ norm: 'a@x', raw: 'A@x' }, { norm: 'b@x', raw: 'B@x' }];
CREATE sh_objarr:2 SET emails = [{ norm: 'c@x', raw: 'C@x' }];
CREATE sh_objarr:3 SET emails = [{ norm: 'b@x', raw: 'B@x' }, { norm: 'd@x', raw: 'D@x' }];

-- G1: CONTAINS over the flattened element field. The single containment
-- value pins the flatten column, so the seek is exact and carries no
-- `Distinct`, as at E1.
EXPLAIN SELECT id FROM sh_objarr WHERE emails.*.norm CONTAINS 'b@x';
SELECT id FROM sh_objarr WHERE emails.*.norm CONTAINS 'b@x';
SELECT id FROM sh_objarr WITH NOINDEX WHERE emails.*.norm CONTAINS 'b@x';

-- G2: the count of G1. The plan is asserted so a silent fall back to a
-- CountScan, which would report the same number here, cannot pass as index use.
EXPLAIN SELECT count() FROM sh_objarr WHERE emails.*.norm CONTAINS 'b@x' GROUP ALL;
SELECT count() FROM sh_objarr WHERE emails.*.norm CONTAINS 'b@x' GROUP ALL;
SELECT count() FROM sh_objarr WITH NOINDEX WHERE emails.*.norm CONTAINS 'b@x' GROUP ALL;

-- G3: equality against the flattened idiom. The idiom yields an array, so no
-- scalar can equal it and the index is not seeked.
EXPLAIN SELECT id FROM sh_objarr WHERE emails.*.norm = 'b@x';
SELECT id FROM sh_objarr WHERE emails.*.norm = 'b@x';
SELECT id FROM sh_objarr WITH NOINDEX WHERE emails.*.norm = 'b@x';

-- G4: a sibling field of the indexed element field is not indexed.
EXPLAIN SELECT id FROM sh_objarr WHERE emails.*.raw CONTAINS 'B@x';
SELECT id FROM sh_objarr WHERE emails.*.raw CONTAINS 'B@x';
SELECT id FROM sh_objarr WITH NOINDEX WHERE emails.*.raw CONTAINS 'B@x';

-- ===========================================================================
-- Shape H: two-level flatten `a.*.b.*`. The column flattens the outer array
-- and traverses `b`; the trailing `*` does not flatten a second time, so the
-- index entries are the inner arrays themselves.
-- ===========================================================================

DEFINE TABLE sh_twolevel SCHEMALESS;
DEFINE INDEX idx ON sh_twolevel COLUMNS a.*.b.*;
CREATE sh_twolevel:1 SET a = [{ b: ['p', 'q'] }, { b: ['r'] }];
CREATE sh_twolevel:2 SET a = [{ b: ['s'] }];

-- H1: CONTAINS a leaf scalar. The seek is narrowed to 'q', but no entry is
-- keyed on a scalar, so nothing matches - and the oracle agrees, because the
-- idiom yields the inner arrays rather than their elements. The count of an
-- empty seek is still planned as an IndexCountScan and still reports a row,
-- carrying zero.
EXPLAIN SELECT id FROM sh_twolevel WHERE a.*.b.* CONTAINS 'q';
SELECT id FROM sh_twolevel WHERE a.*.b.* CONTAINS 'q';
SELECT id FROM sh_twolevel WITH NOINDEX WHERE a.*.b.* CONTAINS 'q';
EXPLAIN SELECT count() FROM sh_twolevel WHERE a.*.b.* CONTAINS 'q' GROUP ALL;
SELECT count() FROM sh_twolevel WHERE a.*.b.* CONTAINS 'q' GROUP ALL;
SELECT count() FROM sh_twolevel WITH NOINDEX WHERE a.*.b.* CONTAINS 'q' GROUP ALL;

-- H2: CONTAINS a whole inner array - the granularity the entries are actually
-- keyed at. The value pins the flatten column, so the seek is exact and
-- carries no `Distinct`; indexed rows, count and oracle must all agree.
EXPLAIN SELECT id FROM sh_twolevel WHERE a.*.b.* CONTAINS ['p', 'q'];
SELECT id FROM sh_twolevel WHERE a.*.b.* CONTAINS ['p', 'q'];
SELECT id FROM sh_twolevel WITH NOINDEX WHERE a.*.b.* CONTAINS ['p', 'q'];
EXPLAIN SELECT count() FROM sh_twolevel WHERE a.*.b.* CONTAINS ['p', 'q'] GROUP ALL;
SELECT count() FROM sh_twolevel WHERE a.*.b.* CONTAINS ['p', 'q'] GROUP ALL;
SELECT count() FROM sh_twolevel WITH NOINDEX WHERE a.*.b.* CONTAINS ['p', 'q'] GROUP ALL;

-- H3: the one-level spelling of the same path reaches the same values, so the
-- two-level index is seeked for it at the same key. Rows, count and oracle
-- agree, as at H2 - the spelling changes the predicate text the scan records,
-- not the entries it reaches.
EXPLAIN SELECT id FROM sh_twolevel WHERE a.*.b CONTAINS ['p', 'q'];
SELECT id FROM sh_twolevel WHERE a.*.b CONTAINS ['p', 'q'];
SELECT id FROM sh_twolevel WITH NOINDEX WHERE a.*.b CONTAINS ['p', 'q'];
EXPLAIN SELECT count() FROM sh_twolevel WHERE a.*.b CONTAINS ['p', 'q'] GROUP ALL;
SELECT count() FROM sh_twolevel WHERE a.*.b CONTAINS ['p', 'q'] GROUP ALL;
SELECT count() FROM sh_twolevel WITH NOINDEX WHERE a.*.b CONTAINS ['p', 'q'] GROUP ALL;

-- ===========================================================================
-- Shape I: computed column `id[1]` - an element of an array record id, so the
-- key is derived from the record id rather than read from the document body.
-- ===========================================================================

DEFINE TABLE sh_idelem SCHEMALESS;
DEFINE INDEX idx ON sh_idelem COLUMNS id[1];
CREATE sh_idelem:[1, 'x'];
CREATE sh_idelem:[2, 'y'];
CREATE sh_idelem:[3, 'x'];

-- I1: equality on the computed column.
EXPLAIN SELECT id FROM sh_idelem WHERE id[1] = 'x';
SELECT id FROM sh_idelem WHERE id[1] = 'x';
SELECT id FROM sh_idelem WITH NOINDEX WHERE id[1] = 'x';

-- I2: range on the computed column.
EXPLAIN SELECT id FROM sh_idelem WHERE id[1] > 'x';
SELECT id FROM sh_idelem WHERE id[1] > 'x';
SELECT id FROM sh_idelem WITH NOINDEX WHERE id[1] > 'x';

-- I3: a different element of the same record id is not the indexed column.
EXPLAIN SELECT id FROM sh_idelem WHERE id[0] = 1;
SELECT id FROM sh_idelem WHERE id[0] = 1;
SELECT id FROM sh_idelem WITH NOINDEX WHERE id[0] = 1;

-- ===========================================================================
-- Shape J: a plain (non-`*`) column whose VALUE is an array. The column shape
-- says scalar, the data says array, and the index resolves that by writing one
-- entry per element - the same physical layout as shape E from a column that
-- never asked for it. The record dedupe of shape E keys off the `*` in the
-- column idiom, which this column does not carry, so a scan of this index is
-- never wrapped in `Distinct` however far its key range reaches - which is
-- what J3 and J2 pin. sh_arrayvalue:1 carries two distinct elements, both
-- inside the range J3 seeks, so fan-out cannot hide.
-- ===========================================================================

DEFINE TABLE sh_arrayvalue SCHEMALESS;
DEFINE INDEX idx ON sh_arrayvalue COLUMNS tags;
CREATE sh_arrayvalue:1 SET tags = ['red', 'tan'];
CREATE sh_arrayvalue:2 SET tags = 'red';
CREATE sh_arrayvalue:3 SET tags = ['ant'];

-- J1: scalar equality. The seek hits sh_arrayvalue:1 through its 'red' entry,
-- and the `predicate:` recheck must reject it, because an array does not equal
-- a scalar. Indexed rows and oracle agree here.
EXPLAIN SELECT id FROM sh_arrayvalue WHERE tags = 'red';
SELECT id FROM sh_arrayvalue WHERE tags = 'red';
SELECT id FROM sh_arrayvalue WITH NOINDEX WHERE tags = 'red';

-- J2: DEFECT PINNED. The count of J1 goes through IndexCountScan, which counts
-- the entries the seek covered and applies no recheck, so it reports the row
-- the row path correctly discarded. Correct answer: the oracle's 1.
EXPLAIN SELECT count() FROM sh_arrayvalue WHERE tags = 'red' GROUP ALL;
SELECT count() FROM sh_arrayvalue WHERE tags = 'red' GROUP ALL;
SELECT count() FROM sh_arrayvalue WITH NOINDEX WHERE tags = 'red' GROUP ALL;

-- J3: DEFECT PINNED, twice over. The range pins nothing and the column carries
-- no `*`, so no `Distinct` is planned above the scan: the range covers both of
-- sh_arrayvalue:1's elements and that record is emitted once per entry. And
-- sh_arrayvalue:3 is dropped, because `['ant'] > 'p'` holds under filter
-- semantics while its only entry, 'ant', sorts below the bound. Correct
-- answer: the oracle's three distinct rows.
EXPLAIN SELECT id FROM sh_arrayvalue WHERE tags > 'p';
SELECT id FROM sh_arrayvalue WHERE tags > 'p';
SELECT id FROM sh_arrayvalue WITH NOINDEX WHERE tags > 'p';

-- J4: the count of J3. It agrees with the oracle only because the duplicated
-- record and the dropped record cancel out - which is why the row assertion
-- above cannot be replaced by a count.
EXPLAIN SELECT count() FROM sh_arrayvalue WHERE tags > 'p' GROUP ALL;
SELECT count() FROM sh_arrayvalue WHERE tags > 'p' GROUP ALL;
SELECT count() FROM sh_arrayvalue WITH NOINDEX WHERE tags > 'p' GROUP ALL;

-- J5: CONTAINS on a plain column. Despite the element-wise entries, the plain
-- column is not offered to CONTAINS, so this falls back to a table scan and is
-- correct.
EXPLAIN SELECT id FROM sh_arrayvalue WHERE tags CONTAINS 'tan';
SELECT id FROM sh_arrayvalue WHERE tags CONTAINS 'tan';
SELECT id FROM sh_arrayvalue WITH NOINDEX WHERE tags CONTAINS 'tan';

-- J6: an unfiltered ORDER BY id over this shape is planned as a table scan,
-- as E6 is over a flatten column with the same entry layout.
EXPLAIN SELECT id FROM sh_arrayvalue ORDER BY id;
SELECT id FROM sh_arrayvalue ORDER BY id;
SELECT id FROM sh_arrayvalue WITH NOINDEX ORDER BY id;
