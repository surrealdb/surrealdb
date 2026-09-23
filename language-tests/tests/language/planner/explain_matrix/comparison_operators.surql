/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Comparison and equality operator matrix against a fixed index set: `=`, `==` (ExactEqual), `!=`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`, `= NONE`, `!= NONE`, `= NULL`, `!= NULL` and a range literal, each with the field on the LEFT and again on the RIGHT (`35 < age`), over a standard scalar index, a UNIQUE scalar index, a nullable scalar index and a plain index on an array column. The planner has a separate bound-inverting arm per operand position, so a slip between the two would return the complement of the right answer while still rendering an IndexScan. Every plan assertion is therefore paired with the same query run twice for rows, once through the index and once `WITH NOINDEX`, which forces a table scan and is the oracle: EXPLAIN shows which operator ran, not whether it returned the right rows. Shapes that deliberately reach no index (`age != 35`, `35 != age`, `email != 'c@x'`, `tags != 'red'`, `score != NULL`, `score IS NOT NULL`) pin their TableScan too, because a shape silently becoming unindexed -- or a negation silently acquiring an IndexScan -- is the regression this file catches. `EXPLAIN` is rejected by the compute-only engine, so the planner axis is narrowed to the two streaming strategies; they plan identically by contract, so both are pinned rather than one. A range literal is exactly a BTree range and is read as one: `age IN 30..40` seeks `access: >=30 <40` and `age IN 30..=40` seeks `>=30 <=40`, each spelling's own bounds carrying which end is open. Three groups record what currently happens rather than what should. (1) A plain (non-`*`) index on an array column fans out to one entry per element, so `tags = 'red'` renders a narrow `access: = 'red'` on the ELEMENT key with the whole-array `predicate: tags = 'red'` retained. The row path re-checks that predicate and correctly returns none -- an array does not equal a scalar -- but the index-only `IndexCountScan` does NOT re-check it and reports 3 where the oracle reports 0. Correct behaviour is 0 from both. (2) A range over the same index emits a row once per matching element and drops rows whose entry falls outside the range: `tags > 'blue'` and `'blue' < tags` return person:d twice and lose person:e, whose empty array is indexed as a single NONE-keyed entry sorting before every string, even though `[] > 'blue'` holds for the filter; `tags >= 'blue'` inflates `count()` the same way. Correct behaviour is the oracle's row set, each row once. (3) The index-only count path over that same fan-out index is wrong independently of the row path: `tags < 'blue'` reports 1 against an oracle of 0 while the ROW path for that same predicate correctly returns none: the one entry below 'blue' is person:e's NONE-keyed empty array, which IndexCountScan counts without ever fetching the record whose whole-array predicate would reject it. Correct behaviour is the oracle in each case. Whole-array equality against this column is the fourth arm of the same disagreement; it belongs to the fan-out key model rather than to the operator axis and is pinned once, in array_fanout_oracle.surql. The cells where index and oracle DO agree are pinned alongside -- the `<`, `<=` and `!=` arms on the array column, `count()` over the UNIQUE index, and `count()` over both NONE sentinel arms -- so a regression in one of them cannot be read as more of the same fan-out noise. In every divergence above the `WITH NOINDEX` result is the correct one. Each plan is asserted on the query without `ORDER BY` and the rows on the same query with `ORDER BY id`, which makes the row comparison deterministic without putting a sort wrapper into every plan; the ordering does not change the access arm."

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
value = "[{ age: 25, email: 'a@x', id: person:a, tags: ['red', 'blue'] }]"

[[test.results]]
value = "[{ age: 30, email: 'b@x', id: person:b, score: NULL, tags: ['green'] }]"

[[test.results]]
value = "[{ age: 35, email: 'c@x', id: person:c, score: 5, tags: ['red'] }]"

[[test.results]]
value = "[{ age: 40, email: 'd@x', id: person:d, score: 10, tags: ['blue', 'green', 'red'] }]"

[[test.results]]
value = "[{ age: 45, email: 'e@x', id: person:e, score: 5, tags: [] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: = 35, direction: Forward, predicate: age = 35]
''''

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: = 35, direction: Forward, predicate: age == 35]
''''

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: age != 35, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: <35, direction: Forward, predicate: age < 35]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: <=35, direction: Forward, predicate: age <= 35]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: >35, direction: Forward, predicate: age > 35]
''''

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: >=35, direction: Forward, predicate: age >= 35]
''''

[[test.results]]
value = "[{ id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: <35, direction: Forward, predicate: 35 > age]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: <=35, direction: Forward, predicate: 35 >= age]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: >35, direction: Forward, predicate: 35 < age]
''''

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: >=35, direction: Forward, predicate: 35 <= age]
''''

[[test.results]]
value = "[{ id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: = 35, direction: Forward, predicate: 35 = age]
''''

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: = 35, direction: Forward, predicate: 35 == age]
''''

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: 35 != age, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_score, access: = NULL, direction: Forward, predicate: score = NULL]
''''

[[test.results]]
value = "[{ id: person:b }]"

[[test.results]]
value = "[{ id: person:b }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_score, access: = NULL, direction: Forward, predicate: score = NULL]
''''

[[test.results]]
value = "[{ id: person:b }]"

[[test.results]]
value = "[{ id: person:b }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: score != NULL, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: score != NULL, pre_decode_filter: yes]
''''

[[test.results]]
value = "[{ id: person:a }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_score, access: = NONE, direction: Forward, predicate: score = NONE]
''''

[[test.results]]
value = "[{ id: person:a }]"

[[test.results]]
value = "[{ id: person:a }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_score, access: >NONE, direction: Forward, predicate: score != NONE]
''''

[[test.results]]
value = "[{ id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_score, access: >NONE, direction: Forward, predicate: NONE != score]
''''

[[test.results]]
value = "[{ id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: >=30 <40, direction: Forward, predicate: age INSIDE 30 .. 40]
''''

[[test.results]]
value = "[{ id: person:b }, { id: person:c }]"

[[test.results]]
value = "[{ id: person:b }, { id: person:c }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_age, access: >=30 <=40, direction: Forward, predicate: age INSIDE 30 ..= 40]
''''

[[test.results]]
value = "[{ id: person:b }, { id: person:c }, { id: person:d }]"

[[test.results]]
value = "[{ id: person:b }, { id: person:c }, { id: person:d }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: = 'c@x', direction: Forward, predicate: email = 'c@x']
"'''

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: = 'c@x', direction: Forward, predicate: 'c@x' = email]
"'''

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = "[{ id: person:c }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: >'c@x', direction: Forward, predicate: email > 'c@x']
"'''

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: <='c@x', direction: Forward, predicate: 'c@x' >= email]
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: email != 'c@x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: = 'red', direction: Forward, predicate: tags = 'red']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: >'blue', direction: Forward, predicate: tags > 'blue']
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:d }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: >'blue', direction: Forward, predicate: 'blue' < tags]
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:d }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: person, condition: tags = 'red']
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: person, condition: tags > 'blue']
"'''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: >='blue', direction: Forward, predicate: tags >= 'blue']
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:d }, { id: person:d }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: person, condition: tags >= 'blue']
"'''

[[test.results]]
value = "[{ count: 7 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: person, condition: age >= 35]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: <'c@x', direction: Forward, predicate: email < 'c@x']
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: <='c@x', direction: Forward, predicate: email <= 'c@x']
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: >='c@x', direction: Forward, predicate: email >= 'c@x']
"'''

[[test.results]]
value = "[{ id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_email, access: >'c@x', direction: Forward, predicate: 'c@x' < email]
"'''

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: person, condition: email >= 'c@x']
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: <'blue', direction: Forward, predicate: tags < 'blue']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: <='blue', direction: Forward, predicate: tags <= 'blue']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: tags != 'red', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: person, condition: 'blue' < tags]
"'''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: person, condition: tags < 'blue']
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: tags != 'red', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: person, condition: score = NONE]
''''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: person, condition: score != NONE]
''''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"
*/
-- One fixed index set for the whole matrix: a standard index on a scalar, a
-- UNIQUE index on a scalar, a standard index on a nullable scalar (so the
-- NONE/NULL arms have somewhere to land), and a standard index on an array
-- column, which fans out to one index entry per element.
DEFINE TABLE person;
DEFINE INDEX idx_age ON person FIELDS age;
DEFINE INDEX idx_email ON person FIELDS email UNIQUE;
DEFINE INDEX idx_score ON person FIELDS score;
DEFINE INDEX idx_tags ON person FIELDS tags;

-- person:a and person:d carry two and three DISTINCT tags: a single-element
-- array would hide duplicate-row emission and count inflation from the
-- fan-out index. person:a leaves `score` unset (NONE), person:b sets it to
-- NULL, and person:e holds an empty array, which the fan-out index records as
-- a single NONE-keyed entry that sorts before every string -- so it is missed
-- by a `>`/`>=` scan and counted by a `<` one.
CREATE person:a SET age = 25, email = 'a@x', tags = ['red', 'blue'];
CREATE person:b SET age = 30, email = 'b@x', score = NULL, tags = ['green'];
CREATE person:c SET age = 35, email = 'c@x', score = 5, tags = ['red'];
CREATE person:d SET age = 40, email = 'd@x', score = 10, tags = ['blue', 'green', 'red'];
CREATE person:e SET age = 45, email = 'e@x', score = 5, tags = [];

-- ==========================================================================
-- Equality with the field on the LEFT. `=` and `==` (ExactEqual) share one
-- planner arm, so both must render the same `access: = 35`.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE age = 35;
SELECT id FROM person WHERE age = 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age = 35 ORDER BY id;

EXPLAIN SELECT id FROM person WHERE age == 35;
SELECT id FROM person WHERE age == 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age == 35 ORDER BY id;

-- ==========================================================================
-- Non-equality against a concrete value has no index expression: negation
-- inverts the result set and a BTree range cannot express it. The TableScan
-- is pinned deliberately -- if this shape ever silently acquires an
-- IndexScan, the rows are almost certainly wrong.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE age != 35;
SELECT id FROM person WHERE age != 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age != 35 ORDER BY id;

-- ==========================================================================
-- Ordering operators with the field on the LEFT.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE age < 35;
SELECT id FROM person WHERE age < 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age < 35 ORDER BY id;

EXPLAIN SELECT id FROM person WHERE age <= 35;
SELECT id FROM person WHERE age <= 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age <= 35 ORDER BY id;

EXPLAIN SELECT id FROM person WHERE age > 35;
SELECT id FROM person WHERE age > 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age > 35 ORDER BY id;

EXPLAIN SELECT id FROM person WHERE age >= 35;
SELECT id FROM person WHERE age >= 35 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age >= 35 ORDER BY id;

-- ==========================================================================
-- The same operators with the field on the RIGHT. The planner has a separate
-- arm per operand position and must INVERT the bound: `35 < age` is
-- `access: >35`, not `<35`. A copy-paste slip between the two arms would
-- return the complement of the right answer while still rendering an
-- IndexScan, which is why each right-hand case is paired with its NOINDEX
-- oracle.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE 35 > age;
SELECT id FROM person WHERE 35 > age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 > age ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 35 >= age;
SELECT id FROM person WHERE 35 >= age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 >= age ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 35 < age;
SELECT id FROM person WHERE 35 < age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 < age ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 35 <= age;
SELECT id FROM person WHERE 35 <= age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 <= age ORDER BY id;

-- Equality is position-symmetric, but it still has to reach the right-hand
-- arm at all rather than falling back to a scan.
EXPLAIN SELECT id FROM person WHERE 35 = age;
SELECT id FROM person WHERE 35 = age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 = age ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 35 == age;
SELECT id FROM person WHERE 35 == age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 == age ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 35 != age;
SELECT id FROM person WHERE 35 != age ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 35 != age ORDER BY id;

-- ==========================================================================
-- NONE and NULL. `IS`/`IS NOT` desugar to `=`/`!=`, so the four spellings
-- collapse onto two planner arms; both spellings are pinned because the
-- desugaring is what makes them equivalent and nothing else asserts it.
--
-- `!= NONE` is the one negation that IS pushed down: NONE sorts first in
-- BTree key order, so it is exactly `> NONE`. `!= NULL` must NOT be pushed,
-- because NONE sorts before NULL and an exclusive `> NULL` scan would drop
-- the NONE rows that `NONE != NULL` makes true.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE score IS NULL;
SELECT id FROM person WHERE score IS NULL ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE score IS NULL ORDER BY id;

EXPLAIN SELECT id FROM person WHERE score = NULL;
SELECT id FROM person WHERE score = NULL ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE score = NULL ORDER BY id;

EXPLAIN SELECT id FROM person WHERE score IS NOT NULL;
SELECT id FROM person WHERE score IS NOT NULL ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE score IS NOT NULL ORDER BY id;

EXPLAIN SELECT id FROM person WHERE score != NULL;
SELECT id FROM person WHERE score != NULL ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE score != NULL ORDER BY id;

EXPLAIN SELECT id FROM person WHERE score = NONE;
SELECT id FROM person WHERE score = NONE ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE score = NONE ORDER BY id;

EXPLAIN SELECT id FROM person WHERE score != NONE;
SELECT id FROM person WHERE score != NONE ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE score != NONE ORDER BY id;

-- `NONE != score` puts the sentinel on the LEFT. The `!= NONE` arm is
-- position-agnostic, so the bound must still come out as `>NONE`.
EXPLAIN SELECT id FROM person WHERE NONE != score;
SELECT id FROM person WHERE NONE != score ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE NONE != score ORDER BY id;

-- ==========================================================================
-- A range literal is exactly what a BTree index range scan expresses, but
-- `field IN a..b` reaches no index arm at all.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE age IN 30..40;
SELECT id FROM person WHERE age IN 30..40 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age IN 30..40 ORDER BY id;

EXPLAIN SELECT id FROM person WHERE age IN 30..=40;
SELECT id FROM person WHERE age IN 30..=40 ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE age IN 30..=40 ORDER BY id;

-- ==========================================================================
-- The same operator set against a UNIQUE index, which is a different index
-- kind reaching the same access arms.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE email = 'c@x';
SELECT id FROM person WHERE email = 'c@x' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE email = 'c@x' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 'c@x' = email;
SELECT id FROM person WHERE 'c@x' = email ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 'c@x' = email ORDER BY id;

EXPLAIN SELECT id FROM person WHERE email > 'c@x';
SELECT id FROM person WHERE email > 'c@x' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE email > 'c@x' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 'c@x' >= email;
SELECT id FROM person WHERE 'c@x' >= email ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 'c@x' >= email ORDER BY id;

EXPLAIN SELECT id FROM person WHERE email != 'c@x';
SELECT id FROM person WHERE email != 'c@x' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE email != 'c@x' ORDER BY id;

-- ==========================================================================
-- Comparison operators against a plain (non-`*`) index whose column holds an
-- array. The index fans out to one entry per element, so the key set the
-- comparison arms seek is a set of ELEMENTS while the predicate the filter
-- path evaluates is about the WHOLE ARRAY. The two disagree, and the
-- divergence is pinned below. Whole-array equality against such a column is
-- the other half of this disagreement and is pinned in
-- array_fanout_oracle.surql, which owns the fan-out key model.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE tags = 'red';
SELECT id FROM person WHERE tags = 'red' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE tags = 'red' ORDER BY id;

-- DEFECT PINNED. The range emits a row once per element above the bound, so
-- person:d comes back twice, and it drops person:e, whose empty array is a
-- single NONE-keyed entry sorting before every string even though
-- `[] > 'blue'` holds for the filter. Correct answer: the oracle's five rows,
-- each once. The same two errors cancel in this case's count(), which is why
-- `tags >= 'blue'` is pinned below as well.
EXPLAIN SELECT id FROM person WHERE tags > 'blue';
SELECT id FROM person WHERE tags > 'blue' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE tags > 'blue' ORDER BY id;

-- DEFECT PINNED, the same wrong rows through the right-hand operand arm. The
-- access range and the wrong answer are identical to the case above, which
-- places the defect in the fan-out model rather than in one operand arm.
-- Correct answer: the oracle's five rows, each once.
EXPLAIN SELECT id FROM person WHERE 'blue' < tags;
SELECT id FROM person WHERE 'blue' < tags ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 'blue' < tags ORDER BY id;

-- count() over a fan-out index goes through IndexCountScan, an index-only
-- path that never fetches a record and so can disagree with both the
-- row-returning index path and the table scan.
-- DEFECT PINNED. IndexCountScan counts the three entries carrying 'red' and
-- never applies the whole-array re-check that makes the row path's empty
-- answer right, so it reports 3. Correct answer: 0, the oracle's count and the
-- length of the row result for the identical predicate above.
EXPLAIN SELECT count() FROM person WHERE tags = 'red' GROUP ALL;
SELECT count() FROM person WHERE tags = 'red' GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE tags = 'red' GROUP ALL;

EXPLAIN SELECT count() FROM person WHERE tags > 'blue' GROUP ALL;
SELECT count() FROM person WHERE tags > 'blue' GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE tags > 'blue' GROUP ALL;

-- `tags >= 'blue'` matches one more element per row than `>` does, so the
-- fan-out entry count and the number of rows whose whole array satisfies the
-- predicate come apart. The `>` case above happens to coincide for two
-- opposite reasons at once, which would let a real count defect hide.
EXPLAIN SELECT id FROM person WHERE tags >= 'blue';
SELECT id FROM person WHERE tags >= 'blue' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE tags >= 'blue' ORDER BY id;

-- DEFECT PINNED. Correct answer: 5, the oracle's count; the recorded 7 is the
-- entry count, inflated by the two extra elements at or above the bound.
EXPLAIN SELECT count() FROM person WHERE tags >= 'blue' GROUP ALL;
SELECT count() FROM person WHERE tags >= 'blue' GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE tags >= 'blue' GROUP ALL;

-- A scalar index for contrast: the count path and the two row paths must all
-- agree when no column fans out.
EXPLAIN SELECT count() FROM person WHERE age >= 35 GROUP ALL;
SELECT count() FROM person WHERE age >= 35 GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE age >= 35 GROUP ALL;

-- ==========================================================================
-- The ordering arms the UNIQUE index has not yet been put through. A UNIQUE
-- index is a different index kind that must reach the same access arms as a
-- standard one, in both operand positions.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE email < 'c@x';
SELECT id FROM person WHERE email < 'c@x' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE email < 'c@x' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE email <= 'c@x';
SELECT id FROM person WHERE email <= 'c@x' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE email <= 'c@x' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE email >= 'c@x';
SELECT id FROM person WHERE email >= 'c@x' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE email >= 'c@x' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE 'c@x' < email;
SELECT id FROM person WHERE 'c@x' < email ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE 'c@x' < email ORDER BY id;

-- The count path over a UNIQUE index, where nothing fans out: index and
-- oracle must agree, which is what makes the fan-out count divergences below
-- attributable to the fan-out rather than to IndexCountScan in general.
EXPLAIN SELECT count() FROM person WHERE email >= 'c@x' GROUP ALL;
SELECT count() FROM person WHERE email >= 'c@x' GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE email >= 'c@x' GROUP ALL;

-- ==========================================================================
-- The lower-bound and negation arms on the fan-out index. Every element is a
-- string and every value of `tags` is an array, and an array sorts after a
-- string, so no whole array satisfies `< 'blue'`: the retained predicate
-- rejects each candidate the element scan produces and the ROW path is
-- correct here, unlike the `>`/`>=` arms above.
-- ==========================================================================
EXPLAIN SELECT id FROM person WHERE tags < 'blue';
SELECT id FROM person WHERE tags < 'blue' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE tags < 'blue' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE tags <= 'blue';
SELECT id FROM person WHERE tags <= 'blue' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE tags <= 'blue' ORDER BY id;

EXPLAIN SELECT id FROM person WHERE tags != 'red';
SELECT id FROM person WHERE tags != 'red' ORDER BY id;
SELECT id FROM person WITH NOINDEX WHERE tags != 'red' ORDER BY id;

EXPLAIN SELECT count() FROM person WHERE 'blue' < tags GROUP ALL;
SELECT count() FROM person WHERE 'blue' < tags GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE 'blue' < tags GROUP ALL;

-- DEFECT PINNED, and the one case where the count path is wrong while the row
-- path for the identical predicate is right: the single entry below 'blue' is
-- person:e's NONE-keyed empty array, which IndexCountScan tallies without ever
-- fetching the record whose whole-array predicate would reject it. Correct
-- answer: 0, the oracle's count.
EXPLAIN SELECT count() FROM person WHERE tags < 'blue' GROUP ALL;
SELECT count() FROM person WHERE tags < 'blue' GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE tags < 'blue' GROUP ALL;

EXPLAIN SELECT count() FROM person WHERE tags != 'red' GROUP ALL;
SELECT count() FROM person WHERE tags != 'red' GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE tags != 'red' GROUP ALL;

-- The count path over the NONE sentinel arms on a scalar index, which are the
-- two arms that are pushed down as a point lookup and an open range.
EXPLAIN SELECT count() FROM person WHERE score = NONE GROUP ALL;
SELECT count() FROM person WHERE score = NONE GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE score = NONE GROUP ALL;

EXPLAIN SELECT count() FROM person WHERE score != NONE GROUP ALL;
SELECT count() FROM person WHERE score != NONE GROUP ALL;
SELECT count() FROM person WITH NOINDEX WHERE score != NONE GROUP ALL;