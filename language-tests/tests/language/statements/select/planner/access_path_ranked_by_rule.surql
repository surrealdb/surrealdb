/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Without statistics a table source's access path is ranked by rule: an equality
on a UNIQUE index, then an equality on another index, then a range, then a
table scan. Under a LIMIT, an index that delivers the ORDER BY outranks a range
over another index, so the read stops once the limit is met and no sort runs;
it does not outrank an equality. Each plan returns the rows the table scan
returns.
"""

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
value = '''
"SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 10]
        Filter [ctx: Db] [predicate: team = 'blue' AND age > 25 AND email = 'p8@example.com']
            IndexScan [ctx: Db] [table: person, index: by_email, access: = 'p8@example.com', direction: Forward]
"'''

[[test.results]]
value = "[{ id: person:8 }]"

[[test.results]]
value = "[{ id: person:8 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 10]
        Filter [ctx: Db] [predicate: age > 25 AND team = 'red']
            IndexScan [ctx: Db] [table: person, index: by_team, access: = 'red', direction: Forward]
"'''

[[test.results]]
value = "[{ id: person:6 }, { id: person:9 }, { id: person:12 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [table: person, index: by_age, access: >30, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: name = 'nobody', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, joined]
    Sort [ctx: Db] [sort_keys: joined ASC]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: person, index: by_age, access: >25, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, joined]
    Limit [ctx: Db] [limit: 3]
        Filter [ctx: Db] [predicate: age > 25]
            DistinctRecords [ctx: Db]
                IndexScan [ctx: Db] [table: person, index: by_joined, direction: Forward]
''''

[[test.results]]
value = "[{ id: person:12, joined: 88 }, { id: person:11, joined: 89 }, { id: person:10, joined: 90 }]"

[[test.results]]
value = "[{ id: person:12, joined: 88 }, { id: person:11, joined: 89 }, { id: person:10, joined: 90 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, joined]
    Limit [ctx: Db] [limit: 2]
        Filter [ctx: Db] [predicate: age < 30]
            DistinctRecords [ctx: Db]
                IndexScan [ctx: Db] [table: person, index: by_joined, direction: Backward]
''''

[[test.results]]
value = "[{ id: person:1, joined: 99 }, { id: person:2, joined: 98 }]"

[[test.results]]
value = "[{ id: person:1, joined: 99 }, { id: person:2, joined: 98 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    CountScan [ctx: Db] [source: person]
''''

[[test.results]]
value = "[{ count: 12 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, joined]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: joined ASC, keep: 2]
            IndexScan [ctx: Db] [table: person, index: by_team, access: = 'red', direction: Forward]
"'''

[[test.results]]
value = "[{ id: person:12, joined: 88 }, { id: person:9, joined: 91 }]"

*/

DEFINE INDEX by_email ON person FIELDS email UNIQUE;
DEFINE INDEX by_team ON person FIELDS team;
DEFINE INDEX by_age ON person FIELDS age;
DEFINE INDEX by_joined ON person FIELDS joined;
FOR $i IN 1..=12 {
	CREATE type::record('person', $i) SET
		email = 'p' + <string> $i + '@example.com',
		team = IF $i % 3 = 0 { 'red' } ELSE { 'blue' },
		age = 20 + $i,
		joined = 100 - $i;
};

-- A unique equality beside an equality and a range: the unique index. With
-- no LIMIT the conjuncts' bitmaps are intersected instead; a LIMIT keeps the
-- streaming read the rules choose.
EXPLAIN SELECT id FROM person WHERE team = 'blue' AND age > 25 AND email = 'p8@example.com' LIMIT 10;
SELECT id FROM person WHERE team = 'blue' AND age > 25 AND email = 'p8@example.com' LIMIT 10;
SELECT id FROM person WITH NOINDEX WHERE team = 'blue' AND age > 25 AND email = 'p8@example.com' LIMIT 10;

-- An equality beside a range: the equality.
EXPLAIN SELECT id FROM person WHERE age > 25 AND team = 'red' LIMIT 10;
SELECT id FROM person WHERE age > 25 AND team = 'red' LIMIT 10;

-- A range alone: the range.
EXPLAIN SELECT id FROM person WHERE age > 30;

-- Nothing an index answers: a table scan.
EXPLAIN SELECT id FROM person WHERE name = 'nobody';

-- Ordered by another index with no LIMIT: the range, and a sort.
EXPLAIN SELECT id, joined FROM person WHERE age > 25 ORDER BY joined;

-- The same under a LIMIT: the index that delivers the order, and no sort.
EXPLAIN SELECT id, joined FROM person WHERE age > 25 ORDER BY joined LIMIT 3;
SELECT id, joined FROM person WHERE age > 25 ORDER BY joined LIMIT 3;
SELECT id, joined FROM person WITH NOINDEX WHERE age > 25 ORDER BY joined LIMIT 3;

-- Descending under a LIMIT, read backwards.
EXPLAIN SELECT id, joined FROM person WHERE age < 30 ORDER BY joined DESC LIMIT 2;
SELECT id, joined FROM person WHERE age < 30 ORDER BY joined DESC LIMIT 2;
SELECT id, joined FROM person WITH NOINDEX WHERE age < 30 ORDER BY joined DESC LIMIT 2;

-- A count reads the COUNT index.
DEFINE INDEX counted ON person COUNT;
EXPLAIN SELECT count() FROM person GROUP ALL;
SELECT count() FROM person GROUP ALL;

-- An equality still outranks the order under a LIMIT.
EXPLAIN SELECT id, joined FROM person WHERE team = 'red' ORDER BY joined LIMIT 2;
SELECT id, joined FROM person WHERE team = 'red' ORDER BY joined LIMIT 2;
