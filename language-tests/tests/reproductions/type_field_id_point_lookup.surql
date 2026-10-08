/**
[env]

[test]
reason = "type::field('id') = <record-id> must use the record-id point lookup (RecordIdScan) like a bare `id`, not a full table scan, even with no user-defined index on id"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: type::field('id') = (item:1)]
        RecordIdScan [ctx: Db] [record_id: item:1]
"'''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: (item:2) = type::field('id')]
        RecordIdScan [ctx: Db] [record_id: item:2]
"'''

[[test.results]]
value = "[{ id: item:2, name: 'Beta', score: 20 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: type::field('id') = (item:3) AND score > 15]
        RecordIdScan [ctx: Db] [record_id: item:3]
"'''

[[test.results]]
value = "[{ id: item:3, name: 'Gamma', score: 30 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: id = (item:1)]
        RecordIdScan [ctx: Db] [record_id: item:1]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: type::field('id') = (item:1)]
        RecordIdScan [ctx: Db] [record_id: item:1]
"'''

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }]"
*/

-- Setup: deliberately NO user-defined index on `id`. The builtin record-id
-- point lookup must apply on its own — the bug was that it only applied after
-- a redundant `DEFINE INDEX ... COLUMNS id` was created.
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- type::field('id') on the LHS must resolve to the record-id point lookup.
EXPLAIN SELECT * FROM item WHERE type::field('id') = item:1;
SELECT * FROM item WHERE type::field('id') = item:1;

-- Reversed operands exercise the `check(right, left)` arm.
EXPLAIN SELECT * FROM item WHERE item:2 = type::field('id');
SELECT * FROM item WHERE item:2 = type::field('id');

-- A residual AND conjunct is still applied after the point lookup.
EXPLAIN SELECT * FROM item WHERE type::field('id') = item:3 AND score > 15;
SELECT * FROM item WHERE type::field('id') = item:3 AND score > 15;
SELECT * FROM item WHERE type::field('id') = item:1 AND score > 15;

-- type::record('...') on the RHS is constant-folded to a record-id literal at
-- plan time, so these also take the point lookup once the LHS is normalized.
EXPLAIN SELECT * FROM item WHERE id = type::record('item:1');
EXPLAIN SELECT * FROM item WHERE type::field('id') = type::record('item:1');
SELECT * FROM item WHERE type::field('id') = type::record('item:1');
